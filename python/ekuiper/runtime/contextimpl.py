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
import sys

from ekuiper.runtime.context import Context


class ContextImpl(Context):

    def __init__(self, meta):
        self.ruleId = meta['ruleId']
        self.opId = meta['opId']
        self.instanceId = meta['instanceId']

    def set_emitter(self, emitter):
        self.emitter = emitter

    def get_rule_id(self):
        return self.ruleId

    def get_op_id(self):
        return self.opId

    def get_instance_id(self):
        return self.instanceId

    def get_logger(self):
        return sys.stdout

    def emit(self, message, meta):
        data = {'message': message, 'meta': meta}
        json_str = json.dumps(data)
        return self.emitter.send(str.encode(json_str))

    def emit_error(self, error):
        data = {'error': error}
        json_str = json.dumps(data)
        return self.emitter.send(str.encode(json_str))