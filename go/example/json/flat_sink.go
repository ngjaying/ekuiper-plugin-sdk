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

// flat the map and print out

package main

import (
	"github.com/lf-edge/ekuiper-plugin-sdk/api"
	"github.com/lf-edge/ekuiper-plugin-sdk/context"
)

type flatSink struct {
}

func (m *flatSink) Configure(props map[string]interface{}) error {
	context.Log.Infof("received config %+v", props)
	return nil
}

func (m *flatSink) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("opening flat sink")
	return nil
}

func (m *flatSink) Collect(ctx api.StreamContext, item interface{}) error {
	ctx.GetLogger().Infof("sink result for rule %s: %s", ctx.GetRuleId(), item)
	return nil
}

func (m *flatSink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("close flat sink")
	return nil
}
