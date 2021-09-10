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
	"bufio"
	"fmt"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pair"
	_ "go.nanomsg.org/mangos/v3/transport/all"
	"log"
	"os/exec"
)

const (
	url     = "ipc:///tmp/rule1_op1.ipc"
	jsonArg = "{\"meta\":{\"ruleId\":\"rule1\",\"opId\":\"op1\",\"instanceId\":0},\"dataSource\":\"hello\",\"config\":{\"a\":1}}"
	//lang = "go"
	//exe = "../go/bin/json.exe"
	lang = "python"
	exe  = "../python/pyjson.py"
)

func main() {
	sock := listen()
	go runPortable(lang, exe, jsonArg)
	for {
		var msg []byte
		// TODO set timeout
		msg, err := sock.Recv()
		if err != nil {
			panic(fmt.Errorf("cannot receive from mangos Socket: %s", err.Error()))
			return
		}
		fmt.Println(string(msg))
	}
}

func runPortable(language string, exe string, jsonArg string) {
	log.Println("executing plugin")
	var cmd *exec.Cmd
	switch language {
	case "go":
		log.Println("starting plugin executable %s with args %s", exe, jsonArg)
		cmd = exec.Command(exe, jsonArg)

	case "python":
		log.Println("starting python plugin executable %s with args %s", exe, jsonArg)
		cmd = exec.Command("python", exe, jsonArg)
	default:
		log.Printf("unsupported language: %s\n", language)
		return
	}
	cmdStdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Println(err)
		return
	}
	go func() {
		scanner := bufio.NewScanner(cmdStdout)
		for scanner.Scan() {
			log.Printf("plugin log: %s\n", scanner.Text())
		}
	}()
	cmdStderr, err := cmd.StderrPipe()
	go func() {
		scanner := bufio.NewScanner(cmdStderr)
		for scanner.Scan() {
			log.Printf("plugin error: %s\n", scanner.Text())
		}
	}()
	if err != nil {
		log.Println(err)
		return
	}
	go func() {
		log.Println("plugin starting")
		err := cmd.Run()
		if err != nil {
			log.Printf("plugin executable %s stops with error %v\n", exe, err)
			return
		}
		process := cmd.Process
		log.Printf("plugin started pid: %d\n", process.Pid)

		defer func() {
			r := recover()

			if err != nil || r != nil {
				cmd.Process.Kill()
			}

			if r != nil {
				panic(r)
			}
		}()
	}()
}

func listen() mangos.Socket {
	var (
		sock mangos.Socket
		err  error
	)
	if sock, err = pair.NewSocket(); err != nil {
		panic(fmt.Errorf("can't get new pull socket: %s", err))
	}
	if err = sock.Listen(url); err != nil {
		panic(fmt.Errorf("can't listen on pull socket for %s: %s", url, err.Error()))
	}
	return sock
}
