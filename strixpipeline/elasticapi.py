import logging
import elasticsearch
from strixpipeline.config import config
from elasticsearch_dsl import Index

es = elasticsearch.Elasticsearch(config.elastic_hosts, timeout=500, retry_on_timeout=True)
_logger = logging.getLogger(__name__)


def setup_alias(alias_name, index_name):
    es.indices.put_alias(index=index_name, name=alias_name)


def delete_index_by_prefix(prefix):
    es.indices.delete(prefix + "_*")


def create_index(index_name):
    index = Index(index_name, using=es)
    index.delete(ignore=404)
    index.create()


def close_index(index_name):
    es.cluster.health(index=index_name, wait_for_status="yellow")
    es.indices.close(index=index_name)


def open_index(index_name):
    es.indices.open(index=index_name)
