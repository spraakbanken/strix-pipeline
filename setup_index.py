import elasticsearch
from elasticsearch import helpers
import config

es = elasticsearch.Elasticsearch(config.elastic_hosts)


def delete_all_of_type(index, es_type):
    body = {
        "filter": {
            "match_all": {}
        }
    }
    ids = map(lambda x: x["_id"] ,es.search(index=index, doc_type=es_type, body=body, size=10000)["hits"]["hits"])

    es_data = [{"_index": index, "_type": es_type, "_id" : authorid, '_op_type': 'delete'} for authorid in ids]
    helpers.bulk(es, es_data)
