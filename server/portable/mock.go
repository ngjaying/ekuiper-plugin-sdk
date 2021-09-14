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
	"context"
	"github.com/lf-edge/ekuiper/pkg/api"
	"log"
	"time"
)

var jsonArg = "{\"meta\":{\"ruleId\":\"rule1\",\"opId\":\"op1\",\"instanceId\":0},\"dataSource\":\"hello\",\"config\":{\"a\":1}}"

type SourceMetadata struct {
	// rule
	RuleId string
	OpId   string
	// plugin
	PluginName string
	PluginType string
	SymbolName string
	Lang       string
	Exe        string
}

type MockContext struct {
	Ctx  context.Context
	Meta *SourceMetadata
}

//Implement context interface
func (c *MockContext) Deadline() (deadline time.Time, ok bool) {
	return c.Ctx.Deadline()
}

func (c *MockContext) Done() <-chan struct{} {
	return c.Ctx.Done()
}

func (c *MockContext) Err() error {
	return c.Ctx.Err()
}

func (c *MockContext) Value(key interface{}) interface{} {
	return c.Ctx.Value(key)
}

// Stream metas
func (c *MockContext) GetContext() context.Context {
	return c.Ctx
}

func (c *MockContext) GetLogger() api.Logger {
	return logger
}

func (c *MockContext) GetRuleId() string {
	return c.Meta.RuleId
}

func (c *MockContext) GetOpId() string {
	return c.Meta.OpId
}

func (c *MockContext) GetInstanceId() int {
	return 0
}

func (c *MockContext) GetRootPath() string {
	//loc, _ := conf.GetLoc("")
	return "root path"
}

func (c *MockContext) SetError(err error) {

}

func (c *MockContext) WithMeta(ruleId string, opId string, _ api.Store) api.StreamContext {
	c.Meta.RuleId = ruleId
	c.Meta.OpId = opId
	return c
}

func (c *MockContext) WithInstance(_ int) api.StreamContext {
	return c
}

func (c *MockContext) WithCancel() (api.StreamContext, context.CancelFunc) {
	ctx, cancel := context.WithCancel(c.Ctx)
	return &MockContext{
		Meta: c.Meta,
		Ctx:  ctx,
	}, cancel
}

func (c *MockContext) IncrCounter(key string, amount int) error {
	return nil
}

func (c *MockContext) GetCounter(key string) (int, error) {
	return 0, nil
}

func (c *MockContext) PutState(key string, value interface{}) error {
	return nil
}

func (c *MockContext) GetState(key string) (interface{}, error) {
	return nil, nil
}

func (c *MockContext) DeleteState(key string) error {
	return nil
}

func (c *MockContext) Snapshot() error {
	return nil
}

func (c *MockContext) SaveState(checkpointId int64) error {
	return nil
}

type Logger struct{}

func (l *Logger) Debug(args ...interface{}) {
	log.Print(args)
}

func (l *Logger) Info(args ...interface{}) {
	log.Print(args)
}
func (l *Logger) Warn(args ...interface{}) {
	log.Print(args)
}

func (l *Logger) Error(args ...interface{}) {
	log.Print(args)
}

func (l *Logger) Debugln(args ...interface{}) {
	log.Println(args)
}

func (l *Logger) Infoln(args ...interface{}) {
	log.Println(args)
}

func (l *Logger) Warnln(args ...interface{}) {
	log.Println(args)
}

func (l *Logger) Errorln(args ...interface{}) {
	log.Println(args)
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	log.Println(args)
}

func (l *Logger) Infof(format string, args ...interface{}) {
	log.Println(args)
}

func (l *Logger) Warnf(format string, args ...interface{}) {
	log.Println(args)
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	log.Println(args)
}

var logger = &Logger{}
