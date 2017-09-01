import elasticsearch
from elasticsearch_dsl import Index, Mapping
from strix.config import config


es = elasticsearch.Elasticsearch(config.elastic_hosts, timeout=60)


def create_sequence_index():
    sequence_index = Index("sequence", using=es)
    if sequence_index.exists():
        sequence_index.delete(ignore=404)
        return

    sequence_index.settings(
        number_of_shards=1,
        number_of_replicas=0
    )
    sequence_index.create()

    m = Mapping("sequence")
    m.meta("_all", enabled=False)
    m.meta("_source", enabled=False)
    m.save("sequence", using=es)


def remove_sequence_index():
    sequence_index = Index("sequence", using=es)
    sequence_index.delete(ignore=404)


def get_id_sequence(index_name, size):
    tasks = "".join(['{"index": {"_index": "sequence", "_type": "sequence", "_id": "' + index_name + '", "_source": {}}}\n{}\n' for _ in range(0, size)])
    result = es.bulk(body=tasks)
    for item in result['items']:
        yield item["index"]["_version"]
