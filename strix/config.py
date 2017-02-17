import yaml
import sys
import os
import logging


class StrixConfig:

    def __init__(self):

        self.logger = logging.getLogger(__name__)

        if "--config" in sys.argv:
            path = sys.argv[sys.argv.index("--config") + 1]
        else:
            path = "config.yml"

        file = open(path)
        self.logger.info("Config file in use: %s", os.path.realpath(file.name))
        self.config = yaml.safe_load(file)

    def __getattr__(self, item):
        try:
            return self.config[item]
        except KeyError:
            self.logger.error("Key: \"%s\" missing from config-file", item)
            raise RuntimeError("Key: \"" + item + "\" missing from config-file")

    def has_attr(self, item):
        return item in self.config


config = StrixConfig()
