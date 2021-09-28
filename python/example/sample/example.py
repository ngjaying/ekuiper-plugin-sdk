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

from pynng import Req0

if __name__ == '__main__':
    s2 = Req0()
    try:
        s2.dial('ipc:///tmp/plugin_$$test.ipc')
        print('dialed')
        s2.send(b'handshake')
        print('sent')
    except:
        print("Unexpected error:", sys.exc_info()[0])
    print('stopping')
    s2.close()
