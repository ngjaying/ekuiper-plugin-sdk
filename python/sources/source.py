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

from abc import abstractmethod

import contextimpl
import emitter
import runtime
from gettext import gettext as _


class Source(object):
    """abstract class for eKuiper source plugin"""

    @abstractmethod
    def configure(self, datasource, conf):
        """configure with the string datasource and conf map and raise error if any"""
        pass

    @abstractmethod
    def open(self, ctx):
        """run continuously and send out the data or error with ctx"""
        pass

    @abstractmethod
    def close(self, ctx):
        """stop running and clean up"""
        pass

    def run(self):
        """setup context, connect to mq and invoke plugin run"""
        c = runtime.parseArgs()
        print("start running py plugin with arg {}".format(c))
        if c.meta.ruleId == "" or c.meta.opId == "":
            msg = _("missing meta ruleId or opId")
            raise ValueError(msg)
        self.configure(c.dataSource, c.config)
        url = "ipc:///tmp/{}_{}.ipc".format(c.meta.ruleId, c.meta.opId)
        self.emitter = emitter.NanomsgSock(url)
        ctx = contextimpl.ContextImpl(c.meta.ruleId, c.meta.opId, c.meta.instanceId, self.emitter)
        self.open(ctx)
        print("closing")
        """todo wait for exit signal"""
        self.close(ctx)