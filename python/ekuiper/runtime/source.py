#  Copyright 2021 EMQ Technologies Co., Ltd.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import sys
import traceback

from ekuiper.runtime.connection import SourceChannel
from ekuiper.runtime.symbol import parse_context


class SourceRuntime:

    def __init__(self, ctrl, s):
        ctx = parse_context(ctrl)
        ds = ""
        config = {}
        if 'datasource' in ctrl:
            ds = ctrl['datasource']
        if 'config' in ctrl:
            config = ctrl['config']
        s.configure(ds, config)
        ch = SourceChannel(ctrl['meta'])
        ctx.set_emitter(ch)
        self.s = s
        self.ctx = ctx
        self.ch = ch
        self.running = False

    def run(self):
        print('start running source')
        self.running = True
        try:
            self.s.open(self.ctx)
        except:
            if self.running:
                print(traceback.format_exc())
        finally:
            self.running = False

    def stop(self):
        self.running = False
        try:
            self.s.close(self.ctx)
            self.ch.close()
        except:
            print(traceback.format_exc())

    def is_running(self):
        return self.running
