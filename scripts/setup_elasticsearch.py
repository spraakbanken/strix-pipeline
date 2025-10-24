import elasticsearch
from snakemake.script import snakemake

from strixpipeline.config import StrixConfig
from strixpipeline.elasticsearch import elasticapi
from strixpipeline.elasticsearch.createindex import create_index

config = StrixConfig()
corpus = snakemake.config.get("corpus")


es = elasticsearch.Elasticsearch(config.elastic_hosts, request_timeout=500, retry_on_timeout=True)

create_index(es, corpus, delete_previous=True)
elasticapi.enable_insert_settings(es, corpus)
