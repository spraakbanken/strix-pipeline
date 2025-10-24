import logging

import elasticsearch
from snakemake.script import snakemake

from strixpipeline.config import StrixConfig
from strixpipeline.elasticsearch import elasticapi

config = StrixConfig()


corpus = snakemake.config.get("corpus")


es = elasticsearch.Elasticsearch(config.elastic_hosts, request_timeout=10000, retry_on_timeout=True)

elasticapi.enable_postinsert_settings(es, corpus, config.number_of_replicas)

_logger = logging.Logger(__name__)


_logger.info("Merging segments")
es.indices.forcemerge(index=corpus + "," + corpus + "_terms", max_num_segments=1)
_logger.info("Done merging segments")
