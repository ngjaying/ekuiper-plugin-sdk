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
	"github.com/lf-edge/ekuiper-plugin-server-sim/shared"
	"github.com/lf-edge/ekuiper/pkg/api"
	"time"
)

const (
	lang = "go"
	exe  = "../go/bin/json.exe"
	//lang = "python"
	//exe  = "../python/pyjson.py"
)

func main() {
	var (
		fs  *portable.PortableFunc
		ss  *portable.PortableSink
		err error
		// sink *portable.PortableSink
	)

	sm := portable.PortableMetadata{
		RuleId:     "rule1",
		OpId:       "op1",
		PluginName: "json",
		PluginType: shared.TYPE_SOURCE,
		SymbolName: "json",
		Lang:       lang,
		Exe:        exe,
	}
	s := portable.NewPortableSource(&sm)
	ctx, cancel := (&portable.MockContext{
		Meta: sm,
		Ctx:  context.Background(),
	}).WithCancel()

	fm := &portable.PortableMetadata{
		RuleId:     "rule1",
		OpId:       "op2",
		PluginName: "json",
		PluginType: shared.TYPE_FUNC,
		SymbolName: "wordcount",
		Lang:       lang,
		Exe:        exe,
	}
	fctx := portable.NewMockFuncContext(ctx.WithMeta(fm.RuleId, fm.OpId, nil), 0)
	fs, err = portable.NewPortableFunc(fctx, fm)
	if err != nil {
		panic(err)
	}

	ssm := &portable.PortableMetadata{
		RuleId:     "rule1",
		OpId:       "op3",
		PluginName: "json",
		PluginType: shared.TYPE_SINK,
		SymbolName: "flat",
		Lang:       lang,
		Exe:        exe,
	}
	ss = portable.NewPortableSink(ssm)
	ss.Configure(map[string]interface{}{"a": 1})
	ssctx := ctx.WithMeta(ssm.RuleId, ssm.OpId, nil)
	err = ss.Open(ssctx)
	if err != nil {
		panic(err)
	}

	sourceOut := make(chan api.SourceTuple)
	funcOut := make(chan interface{})
	errCh := make(chan error)
	go s.Open(ctx.WithMeta(sm.RuleId, sm.OpId, nil), sourceOut, errCh)

	ticker := time.After(10 * time.Second)

outer:
	for {
		select {
		case err := <-errCh:
			portable.Logger.Infof("received error: %v", err)
			cancel()
		case tuple := <-sourceOut:
			portable.Logger.Infof("received from source tuple: %v", tuple)
			if fs != nil {
				if color, ok := tuple.Message()["color"]; ok {
					r, ok := fs.Exec([]interface{}{color}, fctx)
					if !ok {
						portable.Logger.Info("function result error")
					} else {
						go func() {
							funcOut <- r
						}()
					}
				}
			} else if ss != nil {
				err := ss.Collect(ssctx, tuple)
				if err != nil {
					portable.Logger.Info("sink result error %v", err)
				}
			}
		case out := <-funcOut:
			portable.Logger.Infof("received from function: %v", out)
			if ss != nil {
				err := ss.Collect(ssctx, map[string]interface{}{
					"funcOut":   out,
					"processed": true,
				})
				if err != nil {
					portable.Logger.Info("sink result error %v", err)
				}
			}
		case <-ticker:
			portable.Logger.Info("stop after timeout")
			cancel()
			break outer
		}
	}

	defer func() {
		pm := portable.GetPluginInsManager()
		pm.KillAll()
	}()
}
