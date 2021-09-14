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

package main

import (
	"fmt"
	"github.com/lf-edge/ekuiper-plugin-sdk/api"
	"time"
)

var data = []map[string]interface{}{
	{
		"color": "red",
		"size":  3,
		"ts":    1541152486013,
	},
	{
		"color": "yellow",
		"size":  2,
		"ts":    1541152487013,
	},
	{
		"color": "blue",
		"size":  1,
		"ts":    1541152488013,
	},
}

type jsonSource struct {
}

func (s *jsonSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, _ chan<- error) {
	ctx.GetLogger().Infof("Start json source for rule %s", ctx.GetRuleId())
	ticker := time.NewTicker(1 * time.Second)
	c := 0
	for {
		select {
		case <-ticker.C:
			select {
			case consumer <- api.NewDefaultSourceTuple(data[c], nil):
				c = (c + 1) % len(data)
			case <-ctx.Done():
			}
		case <-ctx.Done():
			ticker.Stop()
		}
	}
}

func (s *jsonSource) Configure(dataSource string, config map[string]interface{}) error {
	fmt.Printf("received datasource %s, config %+v", dataSource, config)
	return nil
}

func (s *jsonSource) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing json source")
	return nil
}

// TODO is this eliminatable?
// arg "{\"meta\":{\"ruleId\":\"rule1\",\"opId\":\"op1\",\"instanceId\":0},\"dataSource\":\"hello\",\"config\":{\"a\":1}}"
