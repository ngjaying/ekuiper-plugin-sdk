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
import threading
import traceback

from ekuiper.runtime import shared
from ekuiper.runtime.connection import ControlChannel
from ekuiper.runtime.source import SourceRuntime

reg = {}
conf = None


def start(c):
    init_vars(c)
    global conf
    conf = c
    print("starting plugin {}".format(c.name))
    ch = ControlChannel(c.name)
    ch.run(command_reply)


def init_vars(c):
    # if len(sys.argv) != 2:
    #     msg = gettext('fail to init plugin, must pass exactly 2 args but got {}'.format(sys.argv))
    #     raise ValueError(msg)
    # """TODO validation"""
    # arg = json.loads(sys.argv[1])
    pass


def command_reply(req):
    try:
        cmd = json.loads(req)
        print("receive command {}".format(cmd))
        ctrl = json.loads(cmd['arg'])
        print(ctrl)
        if cmd['cmd'] == shared.CMD_START:
            f = conf.get(ctrl['pluginType'], ctrl['symbolName'])
            if f is None:
                return b'symbol not found'
            s = f()
            if ctrl['pluginType'] == shared.TYPE_SOURCE:
                print("running source {}".format(ctrl['symbolName']))
                runtime = SourceRuntime(ctrl, s)
                x = threading.Thread(target=runtime.run, daemon=True)
                x.start()
                regkey = "{}_{}_{}_{}".format(ctrl['meta']['ruleId'], ctrl['meta']['opId'], ctrl['meta']['instanceId'],
                                              ctrl['symbolName'])
                reg[regkey] = runtime
            elif ctrl['pluginType'] == shared.TYPE_SINK:
                pass
            elif ctrl['pluginType'] == shared.TYPE_FUNC:
                pass
            else:
                return b'invalid plugin type'
        elif cmd['cmd'] == shared.CMD_STOP:
            pass
        return b'ok'
    except:
        var = traceback.format_exc()
        return str.encode(var)


class PluginConfig:

    def __init__(self, name, sources, sinks, functions):
        self.name = name
        self.sources = sources
        self.sinks = sinks
        self.functions = functions

    def get(self, plugin_type, symbol_name):
        if plugin_type == shared.TYPE_SOURCE:
            return self.sources[symbol_name]
        elif plugin_type == shared.TYPE_SINK:
            return self.sinks[symbol_name]
        elif plugin_type == shared.TYPE_FUNC:
            return self.functions[symbol_name]
        else:
            return None
