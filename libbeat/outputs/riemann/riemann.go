// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package riemann

import (
	"bufio"
	"context"
	jsonEncoding "encoding/json"
	"fmt"
	"github.com/Jeffail/gabs"
	"os"
	"runtime"
	"time"

	_ "github.com/Jeffail/gabs"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/outputs/codec/json"
	"github.com/elastic/beats/v7/libbeat/publisher"
	riemanngo "github.com/riemann/riemann-go-client"
	"log"
)

type riemann struct {
	log      *logp.Logger
	out      *os.File
	observer outputs.Observer
	writer   *bufio.Writer
	codec    codec.Codec
	index    string
	hosts    []string
}

type OsData struct {
	Family string
	Kernel string
}

type RiemannData struct {
	Timestamp string
	Hostname  string
	Ip        string
	Os        OsData
	Message   string
	Username  string
	Action    string
}

type consoleEvent struct {
	Timestamp time.Time `json:"@timestamp" struct:"@timestamp"`

	// Note: stdlib json doesn't support inlining :( -> use `codec: 2`, to generate proper event
	Fields interface{} `struct:",inline"`
}

func init() {
	outputs.RegisterType("riemann", makeRiemann)
}

func makeRiemann(
	_ outputs.IndexManager,
	beat beat.Info,
	observer outputs.Observer,
	cfg *common.Config,
) (outputs.Group, error) {
	config := defaultConfig
	err := cfg.Unpack(&config)
	if err != nil {
		return outputs.Fail(err)
	}

	var enc codec.Codec
	if config.Codec.Namespace.IsSet() {
		enc, err = codec.CreateEncoder(beat, config.Codec)
		if err != nil {
			return outputs.Fail(err)
		}
	} else {
		enc = json.New(beat.Version, json.Config{
			EscapeHTML: false,
		})
	}

	hosts, err := outputs.ReadHostList(cfg)
	if err != nil {
		return outputs.Fail(err)
	}

	index := beat.Beat
	c, err := newRiemann(index, observer, enc, hosts)
	if err != nil {
		return outputs.Fail(fmt.Errorf("riemann output initialization failed with: %v", err))
	}

	// check stdout actually being available
	if runtime.GOOS != "windows" {
		if _, err = c.out.Stat(); err != nil {
			err = fmt.Errorf("riemann output initialization failed with: %v", err)
			return outputs.Fail(err)
		}
	}

	return outputs.Success(config.BatchSize, 0, c)
}

func newRiemann(index string, observer outputs.Observer, codec codec.Codec, hosts []string) (*riemann, error) {
	c := &riemann{log: logp.NewLogger("riemann"), out: os.Stdout, codec: codec, observer: observer, index: index, hosts: hosts}
	c.writer = bufio.NewWriterSize(c.out, 8*1024)
	return c, nil
}

func (c *riemann) Close() error { return nil }
func (c *riemann) Publish(_ context.Context, batch publisher.Batch) error {
	st := c.observer
	events := batch.Events()
	st.NewBatch(len(events))

	dropped := 0
	for i := range events {
		ok := c.publishEvent(&events[i])
		if !ok {
			dropped++
		}
	}

	c.writer.Flush()
	batch.ACK()

	st.Dropped(dropped)
	st.Acked(len(events) - dropped)

	return nil
}

var nl = []byte("\n")

func (c *riemann) publishEvent(event *publisher.Event) bool {
	x_times := &event.Content.Fields
	x_fields := x_times.Clone().String()
	//fmt.Println(x_fields,"\n")

	x_jsonParsed, _ := gabs.ParseJSON(jsonEncoding.RawMessage(fmt.Sprintf("%v", x_fields)))
	x_os_data := OsData{
		Family: x_jsonParsed.Path("host.os.family").String(),
		Kernel: x_jsonParsed.Path("host.os.kernel").String(),
	}
	x_riemann_data := RiemannData{
		Timestamp: event.Content.Timestamp.String(),
		Hostname:  x_jsonParsed.Path("host.hostname").String(),
		Os:        x_os_data,
		Message:   x_jsonParsed.Path("message").String(),
		Username:  x_jsonParsed.Path("winlog.user.name").String(),
		Action:    x_jsonParsed.Path("event.action").String(),
	}
	x_riemann_data.sendItToRiemann(c)
	/*fmt.Println(x_jsonParsed.Path("host.hostname").String())*/

	return true
}

func (r *RiemannData) sendItToRiemann(c *riemann) {
	for _, host := range c.hosts {
		conn := riemanngo.NewTCPClient(host, 5*time.Second)
		err := conn.Connect()
		if err != nil {
			panic(err)
		}
		_, err = riemanngo.SendEvent(conn, &riemanngo.Event{
			Service:     "Windows",
			Host:        r.Hostname,
			State:       "ok",
			Metric:      100,
			Description: r.Message,
			Tags:        []string{r.Username},
		})
		if err != nil {
			log.Fatal(err)
		}
		conn.Close()
		time.Sleep(1 * time.Second)
	}
}

func (c *riemann) String() string {
	return "riemann"
}
