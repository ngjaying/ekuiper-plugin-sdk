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
import json

from pynng import Pair0

class NanomsgSock(object):
    def __init__(self, url):
        s = Pair0()
        s.dial(url)
        self.sock = s

    def emit(self, msg, meta):
        data = {'message': msg, 'meta': meta}
        json_str = json.dumps(data)
        self.sock.send(str.encode(json_str))

    def emit_error(self, error):
        data = {'error': error}
        json_str = json.dumps(data)
        self.sock.send(str.encode(json_str))

    def close(self):
        self.sock.close()