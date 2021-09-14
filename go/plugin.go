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
	"github.com/lf-edge/ekuiper-plugin-sdk/shared"
)

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
func Start(_ []string, conf *PluginConfig) {
	fmt.Println("creating control channel")
	ch, err := connection.CreateControlChannel(conf.Name)
	if err != nil {
		panic(err)
	}
	fmt.Println("running control channel")
	err = ch.Run(func(req []byte) []byte { // not parallel run now
		c := &shared.Command{}
		err := json.Unmarshal(req, c)
		if err != nil {
			return []byte(err.Error())
		}
		switch c.Cmd {
		case shared.CMD_START:
			ctrl := &shared.Control{}
			err = json.Unmarshal(c.Arg, ctrl)
			if err != nil {
				return []byte(err.Error())
			}
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
			case TYPE_SINK:
			case TYPE_FUNC:
			default:
				return []byte(shared.REPLY_OK)
			}
			return []byte(shared.REPLY_OK)
		case shared.CMD_STOP:
			return []byte(shared.REPLY_OK)
		default:
			return []byte(fmt.Sprintf("invalid command received: %s", c.Cmd))
		}
	})
	if err != nil {
		fmt.Println(err)
	}
}
