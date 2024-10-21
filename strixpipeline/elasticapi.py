import logging
import elasticsearch
from strixpipeline.config import config
from elasticsearch_dsl import Index

es = elasticsearch.Elasticsearch(
    config.elastic_hosts, request_timeout=500, retry_on_timeout=True
)
_logger = logging.getLogger(__name__)


def get_index_from_alias(alias_name):
    response = es.options(ignore_status=[400, 404]).indices.get_alias(name=alias_name)
    if "status" in response:
        return None
    return list(response.keys())[0]


def setup_alias(alias_name, new_index_name):
    old_index = get_index_from_alias(alias_name)
    if old_index:
        es.indices.delete_alias(name=alias_name, index=old_index)
    es.indices.put_alias(index=new_index_name, name=alias_name)


def delete_index_by_corpus_id(corpus):
    main_index = get_index_from_alias(corpus)
    if main_index:
        es.options(ignore_status=[400, 404]).indices.delete(index=main_index)
    term_index = get_index_from_alias(f"{corpus}_terms")
    if term_index:
        es.options(ignore_status=[400, 404]).indices.delete(index=term_index)


def create_index(index_name):
    index = Index(index_name, using=es)
    index.delete(ignore=404)
    index.create()


def close_index(index_name):
    es.cluster.health(index=index_name, wait_for_status="yellow")
    es.indices.close(index=index_name)


def open_index(index_name):
    es.indices.open(index=index_name)
