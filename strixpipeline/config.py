import logging
import os
import sys

import yaml


class StrixConfig:
    def __init__(self):
        if "--config" in sys.argv:
            path = sys.argv[sys.argv.index("--config") + 1]
        else:
            path = "config.yaml"

        file = open(path)
        logging.getLogger(__name__).info("Config file in use: %s", os.path.realpath(file.name))
        self.config = yaml.safe_load(file)
        self.set_defaults()
        self.create_corpus_config()

    def __getattr__(self, item):
        if item == "__setstate__":
            return super().__getattr__("item")
        if item == "logger":
            return logging.getLogger(__name__)
        if item == "config":
            return self.config

        try:
            return self.config[item]
        except KeyError:
            logging.getLogger(__name__).error('Key: "%s" missing from config-file', item)
            raise RuntimeError('Key: "' + item + '" missing from config-file')

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
