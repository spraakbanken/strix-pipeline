from elasticsearch_dsl import Search
import sys

def check_elastic_search_dsl_version():
    try:
        Search().source
    except AttributeError as e:
        print("""Using untagged features of the elasticsearch-dsl lib, install using
             pip install https://github.com/elastic/elasticsearch-dsl-py/archive/master.zip """,
             file=sys.stderr) # error valid for elasticsearch-dsl version 2.0.0
        raise e