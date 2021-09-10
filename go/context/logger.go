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

package context

import (
	filename "github.com/keepeye/logrus-filename"
	"github.com/lf-edge/ekuiper-plugin-sdk/api"
	"github.com/sirupsen/logrus"
	"os"
)

const (
	logFileName = "stream.log"
)

var (
	Log     *logrus.Logger
	logFile *os.File
)

func InitLogger() api.Logger {
	Log = logrus.New()
	filenameHook := filename.NewHook()
	filenameHook.Field = "file"
	Log.AddHook(filenameHook)

	Log.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		DisableColors:   true,
		FullTimestamp:   true,
	})
	return Log
}

func CloseLogger() {
	if logFile != nil {
		logFile.Close()
	}
}
