import json

from elasticsearch_dsl import Text, Keyword, Index, Object, Integer, Mapping
from strix.pipeline.mappingutil import annotation_analyzer, get_standard_analyzer, get_swedish_analyzer
from strix.config import config
import elasticsearch


class CreateIndex:
    number_of_shards = config.number_of_shards
    number_of_replicas = config.number_of_replicas
    terms_number_of_shards = config.terms_number_of_shards
    terms_number_of_replicas = config.terms_number_of_replicas

    def __init__(self, index):
        self.es = elasticsearch.Elasticsearch(config.elastic_hosts, timeout=120)
        self.index = index
        corpus_config = json.load(open("resources/config/" + index + ".json"))
        self.word_attributes = []
        for attr in corpus_config["analyze_config"]["word_attributes"]:
            self.word_attributes.append(attr)
        for nodeName, attributes in corpus_config["analyze_config"]["struct_attributes"].items():
            for attr in attributes:
                attr["name"] = nodeName + "_" + attr["name"]
                self.word_attributes.append(attr)

        self.text_attributes = corpus_config["analyze_config"]["text_attributes"]

    def create_index(self):
        base_index = Index(self.index, using=self.es)
        base_index.settings(
            number_of_shards=CreateIndex.number_of_shards,
            number_of_replicas=0
        )
        base_index.delete(ignore=404)
        base_index.create()
        self.es.cluster.health(index=self.index, wait_for_status="yellow")
        self.es.indices.close(index=self.index)
        self.create_text_type()
        self.es.indices.open(index=self.index)
        self.create_term_position_index()

    def create_term_position_index(self):
        terms = Index(self.index + "_terms", using=self.es)
        terms.settings(
            number_of_shards=CreateIndex.terms_number_of_shards,
            number_of_replicas=0
        )
        terms.delete(ignore=404)
        terms.create()

        m = Mapping("term")
        m.meta("_all", enabled=False)
        m.meta("dynamic", "strict")

        m.field("position", "integer")
        m.field("term", "object", enabled=False)
        m.field("doc_id", "keyword", index="not_analyzed")
        m.field("doc_type", "keyword", index="not_analyzed")
        m.save(self.index + "_terms", using=self.es)

    def create_text_type(self):
        m = Mapping("text")
        m.meta("_all", enabled=False)
        m.meta("dynamic", "strict")

        text_field = Text(
            analyzer=get_standard_analyzer(),
            term_vector="with_positions_offsets",
            fields={
                "wid": Text(analyzer=annotation_analyzer("wid"), term_vector="with_positions_offsets")
            }
        )

        for attr in self.word_attributes:
            annotation_name = attr["name"]
            text_field.fields[annotation_name] = Text(analyzer=annotation_analyzer(annotation_name, attr["set"]), term_vector="with_positions_offsets")

        m.field("text", text_field)

        for attr in self.text_attributes:
            m.field(attr, Keyword(index="not_analyzed"))

        m.field("dump", Keyword(index="no"))
        m.field("lines", Object(enabled=False))
        m.field("word_count", Integer())

        m.field("title", Text(analyzer=get_swedish_analyzer()))

        m.field("original_file", Keyword())

        m.save(self.index, using=self.es)

    def enable_insert_settings(self):
        self.es.indices.put_settings(index=self.index + "," + self.index + "_terms", body={
            "index.refresh_interval": -1,
        })

    def enable_postinsert_settings(self):
        self.es.indices.put_settings(index=self.index, body={
            "index.number_of_replicas": CreateIndex.number_of_replicas,
            "index.refresh_interval": "30s"
        })
        self.es.indices.put_settings(index=self.index + "_terms", body={
            "index.number_of_replicas": CreateIndex.terms_number_of_replicas,
            "index.refresh_interval": "30s"
        })
