import elasticsearch
import subprocess
from strixpipeline.config import config

es = elasticsearch.Elasticsearch(config.elastic_hosts)
index_name = ".runhistory"


def get_git_commit_id():
    # base_dir = config.base_dir
    # try:
    #     output = subprocess.check_output(["git", "show", "HEAD"], cwd=base_dir).decode("UTF-8")
    # except:
    #     output = "Revision: N/A"
    return "TODO"


def put(obj):
    obj["git_commitid"] = get_git_commit_id()
    es.index(index_name, "entry", obj)


def create():
    if es.indices.exists(index_name):
        return
    else:
        settings = {
            "settings": {
                "index": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                }
            },
            "mappings": {
                "entry": {
                    "properties": {
                        "elastic_hosts": {
                            "properties": {
                                "host": {
                                    "type": "keyword"
                                },
                                "port": {
                                    "type": "long"
                                }
                            }
                        },
                        "index": {
                            "type": "keyword"
                        },
                        "svn_rev": {
                            "type": "keyword"
                        }
                    }
                }
            }
        }
        es.indices.create(index_name, body=settings)
