import json

from elasticsearch_dsl import Text, Keyword, Index, Object, Integer, Mapping
from strix.pipeline.mapping_util import annotation_analyzer, get_standard_analyzer
import strix.config as config
import elasticsearch


class CreateIndex:
    number_of_shards = 1
    number_of_replicas = 0
    terms_number_of_shards = 1
    terms_number_of_replicas = 0

    def __init__(self, index):
        self.es = elasticsearch.Elasticsearch(config.elastic_hosts, timeout=120)
        self.index = index
        corpus_config = json.load(open("resources/config/" + index + ".json"))
        self.word_attributes = corpus_config["analyze_config"]["word_attributes"]
        self.text_attributes = corpus_config["analyze_config"]["text_attributes"]

    def create_index(self):
        base_index = Index(self.index, using=self.es)
        base_index.settings(
            number_of_shards=CreateIndex.number_of_shards,
            number_of_replicas=CreateIndex.number_of_replicas
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
            number_of_replicas=CreateIndex.terms_number_of_replicas
        )
        terms.delete(ignore=404)
        terms.create()

        m = Mapping("term")
        m.meta("_all", enabled=False)

        m.field("position", "integer")
        m.field("term", "object", enabled=False)
        m.field("doc_id", "keyword", index="not_analyzed")
        m.field("doc_type", "keyword", index="not_analyzed")
        m.save(self.index + "_terms", using=self.es)

    def create_text_type(self):
        m = Mapping("text")
        m.meta("_all", enabled=False)
        m.meta("_source", excludes=["text"])

        text_field = Text(
            analyzer=get_standard_analyzer(),
            term_vector="with_positions_offsets",
            fields={
                'wid': Text(analyzer=annotation_analyzer('wid'), term_vector="with_positions_offsets")
            }
        )

        for attr in self.word_attributes:
            annotation_name = attr["name"]
            text_field.fields[annotation_name] = Text(analyzer=annotation_analyzer(annotation_name, attr["set"]), term_vector="with_positions_offsets")

        m.field('text', text_field)

        for attr in self.text_attributes:
            m.field(attr, Keyword(index="not_analyzed"))

        m.field('dump', Keyword(index="no"))
        m.field('lines', Object(enabled=False))

        m.save(self.index, using=self.es)
