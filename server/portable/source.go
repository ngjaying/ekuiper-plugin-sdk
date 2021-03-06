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

package portable

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper-plugin-server-sim/shared"
	"github.com/lf-edge/ekuiper/pkg/api"
)

// Error handling: wrap all error in a function to handle

type DefaultSourceTuple struct {
	Mess map[string]interface{} `json:"message"`
	M    map[string]interface{} `json:"meta"`
}

type PortableSource struct {
	name string
	reg  *PortableMetadata

	topic string
	props map[string]interface{}
}

func NewPortableSource(reg *PortableMetadata) *PortableSource {
	return &PortableSource{
		name:  reg.SymbolName,
		reg:   reg,
		topic: "hello",
		props: map[string]interface{}{"a": 1},
	}
}

func (ps *PortableSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	ctx.GetLogger().Infof("Start running portable source %s with datasource %s and conf %+v", ps.name, ps.topic, ps.props)
	pluginMeta := &Plugin{
		Name:       ps.reg.PluginName,
		Language:   ps.reg.Lang,
		Executable: ps.reg.Exe,
	}
	pm := GetPluginInsManager()
	ins, err := pm.getOrStartProcess(pluginMeta, PortbleConf)
	if err != nil {
		errCh <- err
		return
	}
	ctx.GetLogger().Infof("Plugin started successfully")

	// wait for plugin data
	dataCh, err := CreateSourceChannel(ctx)
	if err != nil {
		errCh <- err
		return
	}
	defer dataCh.Close()

	// Control: send message to plugin to ask starting symbol
	c := &shared.Control{
		Meta: &shared.Meta{
			RuleId:     ctx.GetRuleId(),
			OpId:       ctx.GetOpId(),
			InstanceId: ctx.GetInstanceId(),
		},
		SymbolName: ps.reg.SymbolName,
		PluginType: shared.TYPE_SOURCE,
		DataSource: ps.topic,
		Config:     ps.props,
	}
	err = ins.StartSymbol(ctx, c)
	if err != nil {
		errCh <- err
		return
	}
	defer ins.StopSymbol(ctx, c)

	for {
		var msg []byte
		// TODO set timeout
		msg, err = dataCh.Recv()
		if err != nil {
			errCh <- fmt.Errorf("cannot receive from mangos Socket: %s", err.Error())
			return
		}
		result := &DefaultSourceTuple{}
		e := json.Unmarshal(msg, result)
		if e != nil {
			ctx.GetLogger().Errorf("Invalid data format, cannot decode %s to json format with error %s", string(msg), e)
			continue
		}
		select {
		case consumer <- api.NewDefaultSourceTuple(result.Mess, result.M):
			ctx.GetLogger().Debugf("send data to source node")
		case <-ctx.Done():
			ctx.GetLogger().Info("stop source")
			return
		}
	}
}
