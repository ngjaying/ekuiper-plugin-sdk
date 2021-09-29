
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

from ekuiper.runtime import plugin
from ekuiper.runtime.plugin import PluginConfig
from example.mirror.print import PrintSink
from example.mirror.pyjson import PyJson
from example.mirror.revert import revertIns

if __name__ == '__main__':
    c = PluginConfig("mirror", {"pyjson": lambda: PyJson()}, {"print": lambda: PrintSink()},
                     {"revert": lambda: revertIns})
    plugin.start(c)
