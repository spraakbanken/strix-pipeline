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
        self.set_defaults()

    def __getattr__(self, item):
        try:
            return self.config[item]
        except KeyError:
            self.logger.error("Key: \"%s\" missing from config-file", item)
            raise RuntimeError("Key: \"" + item + "\" missing from config-file")

    def has_attr(self, item):
        return item in self.config

    def set_attr(self, k, v):
        self.config[k] = v

    def set_defaults(self):
        if "base_dir" not in self.config:
            self.config["base_dir"] = "."

    def create_corpus_config(self):
        import strixconfig.corpusconf
        self.config["corpusconf"] = strixconfig.corpusconf.CorpusConfig(config.settings_dir)


config = StrixConfig()

