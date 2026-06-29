import logging
import os
import sys

import yaml

logger = logging.getLogger(__name__)


class StrixConfig:
    def __init__(self):

        if "--config" in sys.argv:
            path = sys.argv[sys.argv.index("--config") + 1]
        else:
            path = "config.yaml"

        with open(path) as file:
            logger.info("Config file in use: %s", os.path.realpath(file.name))
            self.config = yaml.safe_load(file)
        self.set_defaults()
        self.create_corpus_config()

    def __getattr__(self, item):
        try:
            return self.config[item]
        except KeyError:
            logger.error('Key: "%s" missing from config-file', item)
            raise RuntimeError(f'Key: "{item}" missing from config-file')

    def has_attr(self, item):
        return item in self.config

    def set_attr(self, k, v):
        self.config[k] = v

    def set_defaults(self):
        if "base_dir" not in self.config:
            self.config["base_dir"] = "."

    def create_corpus_config(self):
        import strixconfigurer.corpusconf

        self.config["corpusconf"] = strixconfigurer.corpusconf.CorpusConfig(self.settings_dir)


config = StrixConfig()
