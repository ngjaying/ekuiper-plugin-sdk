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
import json
import traceback

from ekuiper.runtime import reg
from ekuiper.runtime.connection import PairChannel
from ekuiper.runtime.contextimpl import ContextImpl
from ekuiper.runtime.symbol import SymbolRuntime


class FunctionRuntime(SymbolRuntime):

    def __init__(self, ctrl, s):
        ch = PairChannel(ctrl['symbolName'], 1)
        self.s = s
        self.ch = ch
        self.running = False
        self.key = "func_{}".format(ctrl['symbolName'])
        self.funcs = {}

    def run(self):
        reg.set(self.key, self)
        try:
            self.ch.run(self.do_run)
        except:
            if self.running:
                print(traceback.format_exc())
        finally:
            self.stop()

    def do_run(self, req):
        try:
            c = json.loads(req)
            print("running func with ", c)
            name = c['func']
            if name == "Validate":
                err = self.s.validate(c['arg'])
                if err != "":
                    return encode_reply(False, err)
                else:
                    return encode_reply(True, "")
            elif name == "Exec":
                args = c['arg']
                if isinstance(args, list) == False or len(args) < 1:
                    return encode_reply(False, 'invalid arg')
                fmeta = json.loads(args[-1])
                if 'ruleId' in fmeta and 'opId' in fmeta and 'instanceId' in fmeta and 'funcId' in fmeta:
                    key = "{}_{}_{}_{}".format(fmeta['ruleId'], fmeta['opId'], fmeta['instanceId'], fmeta['funcId'])
                    if key in self.funcs:
                        fctx = self.funcs[key]
                    else:
                        fctx = ContextImpl(fmeta)
                        self.funcs[key] = fctx
                else:
                    return encode_reply(False,
                                        'invalid arg: {} ruleId, opId, instanceId and funcId are required'.format(
                                            fmeta))
                r = self.s.exec(args[:-1], fctx)
                return encode_reply(True, r)
            elif name == "IsAggregate":
                r = self.s.is_aggregate()
                return encode_reply(True, r)
            else:
                return encode_reply(False, "invalid func {}".format(name))
        except:
            """two occasions: normal stop will close socket to raise an error OR stopped by unexpected error"""
            if self.running:
                print(traceback.format_exc())
                return encode_reply(False, traceback.format_exc())

    def stop(self):
        self.running = False
        try:
            self.ch.close()
            reg.delete(self.key)
        except:
            print(traceback.format_exc())

    def is_running(self):
        return self.running


def encode_reply(state, arg):
    return str.encode(json.dumps({'state': state, 'result': arg}))
