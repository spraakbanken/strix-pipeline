import yaml
import sys
import os
import logging


class StrixConfig:

    def __init__(self):

        logger = logging.getLogger(__name__)

        if "--config" in sys.argv:
            path = sys.argv[sys.argv.index("--config") + 1]
        else:
            path = "config.yml"

        file = open(path)
        logger.info("Config file in use: %s", os.path.realpath(file.name))
        self.config = yaml.safe_load(file)

    def __getitem__(self, key):
        return self.config[key]

    def __getattr__(self, item):
        return self.config[item]


config = StrixConfig()
