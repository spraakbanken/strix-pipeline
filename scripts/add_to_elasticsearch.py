import logging
from pathlib import Path

import elasticsearch
import elasticsearch.helpers
import orjson
from elasticsearch import exceptions, serializer
from snakemake.script import snakemake

from strixpipeline.config import StrixConfig

texts_file = Path(snakemake.input[1])
vector_file = Path(snakemake.input[2])
output_file = Path(snakemake.output[0])
name = snakemake.wildcards.name

corpus = snakemake.config.get("corpus")

config = StrixConfig()


class ORJSONSerializer(serializer.JSONSerializer):
    """Custom serializer using orjson."""

    def dumps(self, data):
        """Serialize data using orjson."""
        if not isinstance(data, (dict, list)):
            raise exceptions.SerializationError(f"Cannot serialize {type(data)}. Must be dict or list.")
        try:
            return orjson.dumps(data).decode("utf-8")
        except Exception as e:
            raise exceptions.SerializationError(f"Orjson serialization error: {e}")

    def loads(self, s):
        """Deserialize data using orjson."""
        try:
            return orjson.loads(s)
        except Exception as e:
            raise exceptions.SerializationError(f"Orjson deserialization error: {e}")


es = elasticsearch.Elasticsearch(
    config.elastic_hosts, request_timeout=500, retry_on_timeout=True, serializer=ORJSONSerializer()
)

# load preprocessed document vectors
transformer_output = {}
with open(vector_file, "rb") as fp:
    for row in fp:
        [doc_id, doc_vector] = orjson.loads(row)
        transformer_output[doc_id] = doc_vector

with open(texts_file, "rb") as fp:
    [texts, terms] = orjson.loads(fp.read())
    for text in texts:
        text["_source"]["sent_vector"] = transformer_output[text["_source"]["doc_id"]]

_logger = logging.getLogger(__name__)


try:
    count = 0
    res = elasticsearch.helpers.streaming_bulk(es, texts + terms)
    for _ in res:
        count += 1
    _logger.info(f"Added {count} documents to index")
except Exception as e:
    _logger.exception(e)
    raise e

Path(output_file).touch()
