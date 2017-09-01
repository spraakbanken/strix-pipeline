import elasticsearch
from strix.config import config

es = elasticsearch.Elasticsearch(config.elastic_hosts)
index_name = ".runhistory"


def put(obj):
    es.index(index_name, "entry", obj)


def create():
    if es.indices.exists(index_name):
        return
    else:
        settings = {
            "number_of_shards": 1,
            "number_of_replicas": 1
        }
        es.indices.create(index_name, body=settings)
