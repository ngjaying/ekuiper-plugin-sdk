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

// Runtime for symbol, to establish data connection

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

type sourceRuntime struct {
	s      api.Source
	ch     connection.DataOutChannel
	ctx    api.StreamContext
	cancel context2.CancelFunc
}

func setupSourceRuntime(con *shared.Control, s api.Source) (*sourceRuntime, error) {
	// init context with args
	ctx, err := parseContext(con)
	// TODO check cmd error handling or using health check
	if err != nil {
		return nil, err
	}
	// init config with args and call source config
	err = s.Configure(con.DataSource, con.Config)
	if err != nil {
		return nil, err
	}
	// connect to mq server
	ch, err := connection.CreateSourceChannel(ctx)
	if err != nil {
		return nil, err
	}
	ctx.GetLogger().Info("Setup message pipeline, start sending")
	ctx, cancel := ctx.WithCancel()
	return &sourceRuntime{
		s:      s,
		ch:     ch,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (s *sourceRuntime) run() {
	defer func() {
		s.cancel()
		s.ch.Close()
		closeSource(s.ctx)
	}()
	errCh := make(chan error)
	consumer := make(chan api.SourceTuple)
	go s.s.Open(s.ctx, consumer, errCh)
	for {
		select {
		case err := <-errCh:
			s.ctx.GetLogger().Errorf("%v", err)
			broadcast(s.ctx, s.ch, err)
			break
		case data := <-consumer:
			s.ctx.GetLogger().Debugf("broadcast data %v", data)
			broadcast(s.ctx, s.ch, data)
		}
	}
}

func closeSource(ctx api.StreamContext) error {
	return nil
}

func parseContext(con *shared.Control) (api.StreamContext, error) {
	if con.Meta.RuleId == "" || con.Meta.OpId == "" {
		err := fmt.Sprintf("invalid arg %v, ruleId and opId are required", con)
		context.Log.Errorf(err)
		return nil, fmt.Errorf(err)
	}
	contextLogger := context.LogEntry("rule", con.Meta.RuleId)
	ctx := context.WithValue(context.Background(), context.LoggerKey, contextLogger).WithMeta(con.Meta.RuleId, con.Meta.OpId, nil)
	return ctx, nil
}

func broadcast(ctx api.StreamContext, sock connection.DataOutChannel, data interface{}) {
	// encode
	var (
		result []byte
		err    error
	)
	switch dt := data.(type) {
	case error:
		result, err = json.Marshal(fmt.Sprintf("{\"error\":\"%v\"}", dt))
		if err != nil {
			ctx.GetLogger().Errorf("%v", err)
			return
		}
	default:
		result, err = json.Marshal(dt)
		if err != nil {
			ctx.GetLogger().Errorf("%v", err)
			return
		}
	}
	if err = sock.Send(result); err != nil {
		ctx.GetLogger().Errorf("Failed publishing: %s", err.Error())
	}
}
