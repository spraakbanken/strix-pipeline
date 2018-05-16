import logging
import elasticsearch
import strixpipeline.createindex as create_index_strix
from strixpipeline.config import config
import strixpipeline.insertdata as insert_data_strix


es = elasticsearch.Elasticsearch(config.elastic_hosts, timeout=500, retry_on_timeout=True)
_logger = logging.getLogger(__name__)


def recreate_indices(indices):
    for index in indices:
        if config.corpusconf.is_corpus(index):
            _delete_index_by_prefix(index)
            ci = create_index_strix.CreateIndex(index)
            try:
                index_name = ci.create_index()
                _setup_alias(index, index_name)
            except elasticsearch.exceptions.TransportError as e:
                _logger.exception("transport error")
                raise e
        else:
            _logger.error("\"" + index + "\" is not a configured corpus")


def _setup_alias(alias_name, index_name):
    es.indices.put_alias(index=index_name, name=alias_name)


def _delete_index_by_prefix(prefix):
    es.indices.delete(prefix + "_*")
