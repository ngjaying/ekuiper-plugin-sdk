// Copyright 2021 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Manage two layers: cmd process & sockets. Each plugin needs a socket, a bunch of plugin only need a process.

package main

import (
	"context"
	"github.com/lf-edge/ekuiper-plugin-server-sim/portable"
	"github.com/lf-edge/ekuiper/pkg/api"
	"log"
	"time"
)

const (
	lang = "go"
	exe  = "../go/bin/json.exe"
	//lang = "python"
	//exe  = "../python/pyjson.py"
)

func main() {
	sm := &portable.SourceMetadata{
		RuleId:     "rule1",
		OpId:       "op1",
		PluginName: "json",
		PluginType: "source",
		SymbolName: "json",
		Lang:       lang,
		Exe:        exe,
	}
	s := portable.NewPortableSource("json", sm)
	ctx, cancel := (&portable.MockContext{
		Meta: sm,
		Ctx:  context.Background(),
	}).WithCancel()
	consumer := make(chan api.SourceTuple)
	errCh := make(chan error)
	go s.Open(ctx, consumer, errCh)

	ticker := time.After(2 * time.Minute)

	for {
		select {
		case err := <-errCh:
			log.Printf("received error: %v\n", err)
			cancel()
		case tuple := <-consumer:
			log.Printf("received tuple: %v\n", tuple)
		case <-ticker:
			log.Print("stop after timeout\n")
			cancel()
			time.Sleep(20 * time.Second)
			break
		}
	}
}
