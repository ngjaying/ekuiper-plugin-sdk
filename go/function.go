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

package sdk

import (
	context2 "context"
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper-plugin-sdk/api"
	"github.com/lf-edge/ekuiper-plugin-sdk/connection"
	"github.com/lf-edge/ekuiper-plugin-sdk/context"
	"github.com/lf-edge/ekuiper-plugin-sdk/shared"
)

type funcRuntime struct {
	s      api.Function
	ch     connection.DataInOutChannel
	ctx    api.FunctionContext
	cancel context2.CancelFunc
	key    string
}

func setupFuncRuntime(con *shared.Control, s api.Function) (*funcRuntime, error) {
	// init context with args
	ctx, err := parseContext(con)
	// TODO check cmd error handling or using health check
	if err != nil {
		return nil, err
	}
	fctx := context.NewDefaultFuncContext(ctx, con.FuncId)
	// connect to mq server
	ch, err := connection.CreateFuncChannel(fctx)
	if err != nil {
		return nil, err
	}
	ctx.GetLogger().Info("setup function channel")
	ctx, cancel := ctx.WithCancel()
	return &funcRuntime{
		s:      s,
		ch:     ch,
		ctx:    fctx,
		cancel: cancel,
		key:    fmt.Sprintf("%s_%s_%d_%d_%s", con.Meta.RuleId, con.Meta.OpId, con.Meta.InstanceId, con.FuncId, con.SymbolName),
	}, nil
}

// TODO how to stop?
func (s *funcRuntime) run() {
	defer s.stop()
	err := s.ch.Run(func(req []byte) []byte {
		d := &shared.FuncData{}
		err := json.Unmarshal(req, d)
		if err != nil {
			return encodeReply(false, err)
		}
		s.ctx.GetLogger().Debugf("running func with %+v", d)
		switch d.Func {
		case "Validate":
			arg, ok := d.Arg.([]interface{})
			if !ok {
				return encodeReply(false, "argument is not interface array")
			}
			err = s.s.Validate(arg)
			if err != nil {
				return encodeReply(true, "")
			} else {
				return encodeReply(false, err.Error())
			}
		case "Exec":
			arg, ok := d.Arg.([]interface{})
			if !ok {
				return encodeReply(false, "argument is not interface array")
			}
			r, b := s.s.Exec(arg, s.ctx)
			return encodeReply(b, r)
		case "IsAggregate":
			result := s.s.IsAggregate()
			return encodeReply(true, fmt.Sprintf("%v", result))
		default:
			return encodeReply(false, fmt.Sprintf("invalid func %s", d.Func))
		}
	})
	s.ctx.GetLogger().Error(err)
}

// TODO multiple error
func (s *funcRuntime) stop() error {
	s.cancel()
	s.ch.Close()
	reg.Delete(s.key)
	return nil
}

func (s *funcRuntime) isRunning() bool {
	return s.ctx.Err() == nil
}

func encodeReply(state bool, arg interface{}) []byte {
	r, _ := json.Marshal(shared.FuncReply{
		State:  state,
		Result: arg,
	})
	return r
}
