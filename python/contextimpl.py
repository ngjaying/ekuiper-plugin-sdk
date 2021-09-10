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

import context

class ContextImpl(context.Context):
    def __init__(self, ruleId, opId, instanceId, emitter):
        self.ruleId = ruleId
        self.opId = opId
        self.instanceId = instanceId
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
        return self.emitter.emit(message, meta)

    def emit_error(self, error):
        return self.emitter.emit(error)