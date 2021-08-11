import yaml
import sys
import os
import logging

import elasticsearch
from elasticsearch.connection import create_ssl_context
import ssl
import urllib3



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
        import strixconfigurer.corpusconf
        self.config["corpusconf"] = strixconfigurer.corpusconf.CorpusConfig(config.settings_dir)


config = StrixConfig()

def get_es_connection():
    if config.connection_type == "insecure_ssl":
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        ssl_context = create_ssl_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        return elasticsearch.Elasticsearch(hosts=config.elastic_hosts,
                            scheme="https",
                            ssl_context=ssl_context,
                            timeout=1200,
                            retry_on_timeout=True,
                            http_auth=config.http_auth)
    else:
        elasticsearch.Elasticsearch(config.elastic_hosts, timeout=120)
        