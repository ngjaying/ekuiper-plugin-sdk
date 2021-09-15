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

type PortableFunc struct {
	name   string
	reg    *PortableMetadata
	ctx    api.FunctionContext
	dataCh DataReqChannel
}

func NewPortableFunc(ctx api.FunctionContext, reg *PortableMetadata) (*PortableFunc, error) {
	// Setup channel and route the data
	Logger.Infof("Start running portable function meta %+v", reg)
	pluginMeta := &Plugin{
		Name:       reg.PluginName,
		Language:   reg.Lang,
		Executable: reg.Exe,
	}
	pm := GetPluginInsManager()
	ins, err := pm.getOrStartProcess(pluginMeta, PortbleConf)
	if err != nil {
		return nil, err
	}
	Logger.Infof("Plugin started successfully")

	// Create function channel
	dataCh, err := CreateFunctionChannel(ctx)
	if err != nil {
		return nil, err
	}

	// Start symbol
	c := &shared.Control{
		Meta: &shared.Meta{
			RuleId:     ctx.GetRuleId(),
			OpId:       ctx.GetOpId(),
			InstanceId: ctx.GetInstanceId(),
		},
		SymbolName: reg.SymbolName,
		PluginType: shared.TYPE_FUNC,
		FuncId:     0,
	}
	err = ins.StartSymbol(ctx, c)
	if err != nil {
		return nil, err
	}

	// TODO handshake timeout
	err = dataCh.Handshake()
	if err != nil {
		return nil, fmt.Errorf("function %s handshake error: %v", reg.SymbolName, err)
	}
	go func() {
		select {
		case <-ctx.Done():
			dataCh.Close()
			ins.StopSymbol(ctx, c)
		}
	}()

	return &PortableFunc{
		name:   reg.SymbolName,
		reg:    reg,
		dataCh: dataCh,
		ctx:    ctx,
	}, nil
}

func (f *PortableFunc) Validate(args []interface{}) error {
	// TODO function arg encoding
	jsonArg, err := encode("Validate", args)
	if err != nil {
		return err
	}
	res, err := f.dataCh.Req(jsonArg)
	if err != nil {
		return err
	}
	fr := &shared.FuncReply{}
	err = json.Unmarshal(res, fr)
	if err != nil {
		return err
	}
	if fr.State {
		r, ok := fr.Result.(string)
		if ok {
			if r == shared.REPLY_OK {
				return nil
			} else {
				return fmt.Errorf(r)
			}
		} else {
			return fmt.Errorf("validate return result is not string, got %+v", fr)
		}
	} else {
		return fmt.Errorf("validate return state is false, got %+v", fr)
	}

}

func (f *PortableFunc) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
	ctx.GetLogger().Debugf("running portable func with args %+v", args)
	jsonArg, err := encode("Exec", args)
	if err != nil {
		return err, false
	}
	res, err := f.dataCh.Req(jsonArg)
	if err != nil {
		return err, false
	}
	fr := &shared.FuncReply{}
	err = json.Unmarshal(res, fr)
	if err != nil {
		return err, false
	}
	return fr.Result, fr.State
}

func (f *PortableFunc) IsAggregate() bool {
	// TODO error handling
	jsonArg, err := encode("IsAggregate", nil)
	if err != nil {
		f.ctx.GetLogger().Error(err)
		return false
	}
	res, err := f.dataCh.Req(jsonArg)
	if err != nil {
		f.ctx.GetLogger().Error(err)
		return false
	}
	fr := &shared.FuncReply{}
	err = json.Unmarshal(res, fr)
	if err != nil {
		f.ctx.GetLogger().Error(err)
		return false
	}
	if fr.State {
		r, ok := fr.Result.(bool)
		if !ok {
			f.ctx.GetLogger().Errorf("IsAggregate result is not bool, got %v", res)
			return false
		} else {
			return r
		}
	} else {
		f.ctx.GetLogger().Errorf("IsAggregate return state is false, got %+v", fr)
		return false
	}
}

func encode(funcName string, arg interface{}) ([]byte, error) {
	c := shared.FuncData{
		Func: funcName,
		Arg:  arg,
	}
	return json.Marshal(c)
}
