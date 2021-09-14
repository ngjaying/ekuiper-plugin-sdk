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

// Plugin runtime to control the whole plugin with control channel: Distribute symbol data connection, stop symbol and stop plugin

package sdk

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper-plugin-sdk/api"
	"github.com/lf-edge/ekuiper-plugin-sdk/connection"
	"github.com/lf-edge/ekuiper-plugin-sdk/context"
	"github.com/lf-edge/ekuiper-plugin-sdk/shared"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var (
	logger api.Logger
	reg    runtimes
)

func initVars(conf *PluginConfig) {
	logger = context.LogEntry("plugin", conf.Name)
	reg = runtimes{
		content: make(map[string]RuntimeInstance),
		RWMutex: sync.RWMutex{},
	}
}

type NewSourceFunc func() api.Source

// PluginConfig construct once and then read only
type PluginConfig struct {
	Name    string
	Sources map[string]NewSourceFunc
}

const (
	TYPE_SOURCE   = "source"
	TYPE_SINK     = "sink"
	TYPE_FUNC     = "func"
	TYPE_NOTFOUND = "none"
)

func (conf *PluginConfig) Get(symbolName string) (pluginType string, builderFunc interface{}) {
	if f, ok := conf.Sources[symbolName]; ok {
		return TYPE_SOURCE, f
	}
	//if f, ok := conf.Sources[symbolName]; ok {
	//	return TYPE_SINK
	//}
	return TYPE_NOTFOUND, nil
}

// Start Connect to control plane
// Only run once at process startup
// TODO parse configuration like debug mode
func Start(_ []string, conf *PluginConfig) {
	initVars(conf)
	logger.Info("starting plugin, creating control channel")
	ch, err := connection.CreateControlChannel(conf.Name)
	if err != nil {
		panic(err)
	}
	go func() {
		logger.Info("running control channel")
		err = ch.Run(func(req []byte) []byte { // not parallel run now
			c := &shared.Command{}
			err := json.Unmarshal(req, c)
			if err != nil {
				return []byte(err.Error())
			}
			logger.Infof("received command %s with arg:'%s'", c.Cmd, c.Arg)
			ctrl := &shared.Control{}
			err = json.Unmarshal(c.Arg, ctrl)
			if err != nil {
				return []byte(err.Error())
			}
			regKey := fmt.Sprintf("%s_%s_%d_%s", ctrl.Meta.RuleId, ctrl.Meta.OpId, ctrl.Meta.InstanceId, ctrl.SymbolName)
			switch c.Cmd {
			case shared.CMD_START:
				pt, f := conf.Get(ctrl.SymbolName)
				switch pt {
				case TYPE_SOURCE:
					sf := f.(NewSourceFunc)
					sr, err := setupSourceRuntime(ctrl, sf())
					if err != nil {
						return []byte(err.Error())
					}
					// TODO need to know how many are running
					go sr.run()
					reg.Set(regKey, sr)
					logger.Infof("running source %s", ctrl.SymbolName)
				case TYPE_SINK:
				case TYPE_FUNC:
				default:
					return []byte("symbol not found")
				}
				return []byte(shared.REPLY_OK)
			case shared.CMD_STOP:
				logger.Infof("stopping %s", regKey)
				runtime, ok := reg.Get(regKey)
				if !ok {
					return []byte(fmt.Sprintf("symbol %s not found", regKey))
				}
				if runtime.isRunning() {
					err = runtime.stop()
					if err != nil {
						return []byte(err.Error())
					}
				}
				return []byte(shared.REPLY_OK)
			default:
				return []byte(fmt.Sprintf("invalid command received: %s", c.Cmd))
			}
		})
		if err != nil {
			logger.Error(err)
		}
		os.Exit(1)
	}()
	//Stop the whole plugin
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	<-sigint
	logger.Infof("stopping plugin %s", conf.Name)
	os.Exit(0)
}

// key is rule_op_ins_symbol
type runtimes struct {
	content map[string]RuntimeInstance
	sync.RWMutex
}

func (r *runtimes) Set(name string, instance RuntimeInstance) {
	r.Lock()
	defer r.Unlock()
	r.content[name] = instance
}

func (r *runtimes) Get(name string) (RuntimeInstance, bool) {
	r.RLock()
	defer r.RUnlock()
	result, ok := r.content[name]
	return result, ok
}

func (r *runtimes) Delete(name string) {
	r.Lock()
	defer r.Unlock()
	delete(r.content, name)
}
