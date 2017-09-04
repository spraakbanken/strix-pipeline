import elasticsearch
import subprocess
from strix.config import config

es = elasticsearch.Elasticsearch(config.elastic_hosts)
index_name = ".runhistory"


def get_svn_revision():
    base_dir = config.base_dir
    try:
        output = subprocess.check_output(["svn", "info"], cwd=base_dir, stderr=subprocess.STDOUT).decode("UTF-8")
    except subprocess.CalledProcessError:
        output = subprocess.check_output(["git", "svn", "info"], cwd=base_dir).decode("UTF-8")
    except:
        output = "Revision: N/A"
    lines = output.split("\n")
    rev_line = filter(lambda line: line.startswith("Revision"), lines)
    return list(next(rev_line).split(": "))[1]


def put(obj):
    obj["svn_rev"] = get_svn_revision()
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
