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
from concurrent.futures import thread

from pynng import Req0, Push0


class ControlChannel:

    def __init__(self, plugin_name):
        s = Req0()
        """TODO options"""
        url = "ipc:///tmp/plugin_{}.ipc".format(plugin_name)
        s.dial(url)
        self.sock = s

    """ run this in a new thread"""

    def run(self, reply_func):
        self.sock.send(b'handshake')
        while True:
            msg = self.sock.recv()
            reply = reply_func(msg)
            self.sock.send(reply)

    def close(self):
        self.sock.close()


class SourceChannel:

    def __init__(self, meta):
        s = Push0()
        url = "ipc:///tmp/{}_{}_{}.ipc".format(meta['ruleId'], meta['opId'], meta['instanceId'])
        print(url)
        s.dial(url)
        self.sock = s

    def send(self, data):
        self.sock.send(data)

    def close(self):
        self.sock.close()
