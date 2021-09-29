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
import traceback

from ekuiper.runtime import reg
from ekuiper.runtime.connection import SinkChannel
from ekuiper.runtime.symbol import SymbolRuntime, parse_context


class SinkRuntime(SymbolRuntime):

    def __init__(self, ctrl, s):
        ctx = parse_context(ctrl)
        config = {}
        if 'config' in ctrl:
            config = ctrl['config']
        s.configure(config)
        ch = SinkChannel(ctrl['meta'])
        self.s = s
        self.ctx = ctx
        self.ch = ch
        self.running = False
        self.key = "{}_{}_{}_{}".format(ctrl['meta']['ruleId'], ctrl['meta']['opId'], ctrl['meta']['instanceId'],
                                        ctrl['symbolName'])

    def run(self):
        print('start running sink')
        try:
            self.s.open(self.ctx)
            self.running = True
            reg.set(self.key, self)
            while True:
                msg = self.ch.recv()
                self.s.collect(self.ctx, msg)
        except:
            """two occasions: normal stop will close socket to raise an error OR stopped by unexpected error"""
            if self.running:
                print(traceback.format_exc())
        finally:
            self.stop()

    def stop(self):
        self.running = False
        try:
            self.s.close(self.ctx)
            self.ch.close()
            reg.delete(self.key)
        except:
            print(traceback.format_exc())

    def is_running(self):
        return self.running
