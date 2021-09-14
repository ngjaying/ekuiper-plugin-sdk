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

package shared

type Meta struct {
	RuleId     string `json:"ruleId"`
	OpId       string `json:"opId"`
	InstanceId int    `json:"instanceId"`
}

type Control struct {
	SymbolName string                 `json:"symbolName"`
	Meta       *Meta                  `json:"meta,omitempty"`
	DataSource string                 `json:"dataSource,omitempty"`
	Config     map[string]interface{} `json:"config,omitempty"`
}

type Command struct {
	Cmd string `json:"cmd"`
	Arg []byte `json:"arg"`
}

const (
	CMD_START = "start"
	CMD_STOP  = "stop"
)

const (
	REPLY_OK = "ok"
)
