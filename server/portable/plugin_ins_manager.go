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
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper-plugin-server-sim/shared"
	"github.com/lf-edge/ekuiper/pkg/api"
	"io"
	"os"
	"os/exec"
	"sync"
)

var (
	once sync.Once
	pm   *pluginInsManager
)

// TODO plugin instance close
// TODO timeout handling

type pluginIns struct {
	process      *os.Process
	ctrlChan     ControlChannel
	runningCount int
	name         string
}

func (i *pluginIns) StartSymbol(ctx api.StreamContext, ctrl *shared.Control) error {
	arg, err := json.Marshal(ctrl)
	if err != nil {
		return err
	}
	c := shared.Command{
		Cmd: shared.CMD_START,
		Arg: arg,
	}
	jsonArg, err := json.Marshal(c)
	if err != nil {
		return err
	}
	err = i.ctrlChan.SendCmd(jsonArg)
	if err == nil {
		i.runningCount++
		ctx.GetLogger().Infof("started symbol %s", ctrl.SymbolName)
	}
	return err
}

func (i *pluginIns) StopSymbol(ctx api.StreamContext, ctrl *shared.Control) error {
	arg, err := json.Marshal(ctrl)
	if err != nil {
		return err
	}
	c := shared.Command{
		Cmd: shared.CMD_STOP,
		Arg: arg,
	}
	jsonArg, err := json.Marshal(c)
	if err != nil {
		return err
	}
	err = i.ctrlChan.SendCmd(jsonArg)
	i.runningCount--
	ctx.GetLogger().Infof("stopped symbol %s", ctrl.SymbolName)
	if i.runningCount == 0 {
		err := GetPluginInsManager().Kill(i.name)
		if err != nil {
			ctx.GetLogger().Infof("fail to stop plugin %s: %v", i.name, err)
			return err
		}
		ctx.GetLogger().Infof("stop plugin %s", i.name)
	}
	return err
}

func (i *pluginIns) Stop() error {
	i.ctrlChan.Close()
	err := i.process.Kill()
	return err
}

// Manager plugin process and control socket
type pluginInsManager struct {
	instances map[string]*pluginIns
	sync.RWMutex
}

func GetPluginInsManager() *pluginInsManager {
	once.Do(func() {
		pm = &pluginInsManager{
			instances: make(map[string]*pluginIns),
		}
	})
	return pm
}

func (p *pluginInsManager) getPluginIns(name string) (*pluginIns, bool) {
	p.RLock()
	defer p.RUnlock()
	ins, ok := p.instances[name]
	return ins, ok
}

func (p *pluginInsManager) getOrStartProcess(pluginMeta *Plugin) (*pluginIns, error) {
	p.Lock()
	defer p.Unlock()
	if ins, ok := p.instances[pluginMeta.Name]; ok {
		return ins, nil
	}

	Logger.Println("create control channel")
	ctrlChan, err := CreateControlChannel(pluginMeta.Name)
	if err != nil {
		return nil, fmt.Errorf("can't create new control channel: %s", err.Error())
	}

	Logger.Println("executing plugin")
	var cmd *exec.Cmd
	switch pluginMeta.Language {
	case "go":
		Logger.Printf("starting go plugin executable %s\n", pluginMeta.Executable)
		cmd = exec.Command(pluginMeta.Executable)

	case "python":
		Logger.Printf("starting python plugin executable %s\n", pluginMeta.Executable)
		cmd = exec.Command("python", pluginMeta.Executable)
	default:
		return nil, fmt.Errorf("unsupported language: %s\n", pluginMeta.Language)
	}
	cmdStdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	go logging(cmdStdout)
	cmdStderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	go logging(cmdStderr)

	Logger.Println("plugin starting")
	err = cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("plugin executable %s stops with error %v", pluginMeta.Executable, err)
	}
	process := cmd.Process
	Logger.Printf("plugin started pid: %d\n", process.Pid)
	go func() {
		err = cmd.Wait()
		if err != nil {
			Logger.Printf("plugin executable %s stops with error %v", pluginMeta.Executable, err)
		}
		p.Lock()
		defer p.Unlock()
		if ins, ok := p.getPluginIns(pluginMeta.Name); ok {
			ins.ctrlChan.Close()
			delete(p.instances, pluginMeta.Name)
		}
	}()

	Logger.Println("waiting handshake")
	err = ctrlChan.Handshake()
	if err != nil {
		return nil, fmt.Errorf("plugin %s control handshake error: %v\n", pluginMeta.Executable, err)
	}

	ins := &pluginIns{
		name:     pluginMeta.Name,
		process:  process,
		ctrlChan: ctrlChan,
	}
	p.instances[pluginMeta.Name] = ins
	Logger.Println("plugin start running")
	return ins, nil
}

func (p *pluginInsManager) Kill(name string) error {
	p.Lock()
	defer p.Unlock()
	var err error
	if ins, ok := p.instances[name]; ok {
		err = ins.Stop()
		delete(p.instances, name)
	} else {
		return fmt.Errorf("instance %s not found", name)
	}
	return err
}

func (p *pluginInsManager) KillAll() error {
	p.Lock()
	defer p.Unlock()
	for _, ins := range p.instances {
		ins.Stop()
	}
	p.instances = make(map[string]*pluginIns)
	return nil
}

// ends when the plugin process end, we will receive EOF
func logging(r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fmt.Printf("plugin log: %s\n", scanner.Text())
	}
	Logger.Println("stop plugin logging")
}
