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
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper-plugin-sdk/api"
	"github.com/lf-edge/ekuiper-plugin-sdk/context"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pair"
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

// Start Run the mq client and init the
func Start(args []string, s api.Source) {
	// init context with args
	ctx, con, err := parseContext(args)
	// TODO check cmd error handling or using health check
	if err != nil {
		return
	}
	// init config with args and call source config
	err = s.Configure(con.DataSource, con.Config)
	if err != nil {
		return
	}
	// connect to mq server
	var (
		sock mangos.Socket
		url  = fmt.Sprintf("ipc:///tmp/%s_%s.ipc", ctx.GetRuleId(), ctx.GetOpId())
	)
	if sock, err = pair.NewSocket(); err != nil {
		ctx.GetLogger().Errorf("can't get new push socket: %s", err.Error())
		return
	}
	if err = sock.Dial(url); err != nil {
		ctx.GetLogger().Errorf("can't dial on push socket: %s", err.Error())
		return
	}
	defer sock.Close()
	ctx.GetLogger().Info("Setup message pipeline, start sending")
	// run open in another go routine and send data to the server channel
	// while waiting for close
	ctx, cancel := ctx.WithCancel()
	errCh := make(chan error)
	consumer := make(chan api.SourceTuple)
	go s.Open(ctx, consumer, errCh)
	for {
		select {
		case err := <-errCh:
			ctx.GetLogger().Errorf("%v", err)
			cancel()
			broadcast(ctx, sock, err)
			return
		case data := <-consumer:
			ctx.GetLogger().Debugf("broadcast data %v", data)
			broadcast(ctx, sock, data)
		}
	}
}

func broadcast(ctx api.StreamContext, sock mangos.Socket, data interface{}) {
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

func parseContext(args []string) (api.StreamContext, *control, error) {
	contextLogger := context.InitLogger()
	if len(args) != 2 {
		err := fmt.Sprintf("fail to init plugin, must pass exactly 2 os.Arg but got %+v", args)
		contextLogger.Errorf(err)
		return nil, nil, fmt.Errorf(err)
	}
	c := context.WithValue(context.Background(), context.LoggerKey, contextLogger)
	con, err := parse(args[1])
	if err != nil {
		contextLogger.Errorf("%v", err)
		return nil, nil, err
	}
	if con.Meta.RuleId == "" || con.Meta.OpId == "" {
		err := fmt.Sprintf("fail to parse '%s', ruleId and opId are required", args[1])
		contextLogger.Errorf(err)
		return nil, nil, fmt.Errorf(err)
	}
	return c.WithMeta(con.Meta.RuleId, con.Meta.OpId, nil), con, nil
}
