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
#  limitations under the License
import json
import sys
from gettext import gettext
from types import SimpleNamespace


def parseArgs():
    if len(sys.argv) != 2:
        msg = gettext('fail to init plugin, must pass exactly 2 args but got {}'.format(sys.argv))
        raise ValueError(msg)
    """TODO validation"""
    c = json.loads(sys.argv[1], object_hook=lambda d: SimpleNamespace(**d))
    return c

