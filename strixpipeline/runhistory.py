import subprocess

from strixpipeline import elasticapi
from strixpipeline.config import config

index_name = ".runhistory"


def get_git_commit_id():
    base_dir = config.base_dir
    try:
        output = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=base_dir).decode("UTF-8").strip()
    except Exception:
        output = "Revision: N/A"
    return output


def put(obj):
    obj["git_commitid"] = get_git_commit_id()
    elasticapi.es.index(index=index_name, document=obj)


def create():
    mappings = {
        "properties": {
            "elastic_hosts": {"properties": {"host": {"type": "keyword"}, "port": {"type": "long"}}},
            "index": {"type": "keyword"},
            "git_commitid": {"type": "keyword"},
        }
    }
    settings = {
        "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 1}},
        "mappings": mappings,
    }
    if elasticapi.es.indices.exists(index=index_name):
        elasticapi.es.indices.put_mapping(index=index_name, body=mappings)
    else:
        elasticapi.es.indices.create(index=index_name, body=settings)
