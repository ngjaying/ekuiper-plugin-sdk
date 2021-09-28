
from ekuiper.runtime import plugin
from ekuiper.runtime.plugin import PluginConfig
from example.mirror.pyjson import PyJson

if __name__ == '__main__':
    c = PluginConfig("$$test", {"pyjson": lambda: PyJson()}, {}, {})
    plugin.start(c)
