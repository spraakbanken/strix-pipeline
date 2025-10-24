import logging
import os

import elasticsearch

from strixpipeline.config import StrixConfig

_logger = logging.getLogger(__name__)


def _get_indices_from_alias(es, alias_name):
    aliases = es.cat.aliases(name=[alias_name + "*"], format="json")

    alias_exist = False
    index_names = []
    for alias in aliases:
        if alias["alias"] in [alias_name, f"{alias_name}_terms"]:
            alias_exist = True
            index_names.append(alias["index"])

    if not alias_exist:
        _logger.info(f'Alias "{alias_name}", does not exist')
    return index_names


def _remove_config_file(config, corpus):
    settings_dir = config.settings_dir
    fname = os.path.join(settings_dir, f"corpora/{corpus}.yaml")
    if os.path.isfile(fname):
        _logger.info(f"Deleting configuration file: {fname}")
        os.remove(fname)
    else:
        _logger.info(f"Corpus file: '{fname}' does not exist")


def do_delete(corpus):
    config = StrixConfig()
    es = elasticsearch.Elasticsearch(config.elastic_hosts, request_timeout=500, retry_on_timeout=True)

    # We expect that an alias only points to *one* index, but if it points to multiple, just remove all of them
    main_indices = _get_indices_from_alias(es, corpus)
    for index in main_indices:
        _logger.info(f"Deleting index: {index}")
        es.indices.delete(index=index)
        _logger.info("Done deleting index")

    _remove_config_file(config, corpus)
