import time

from elasticsearch_dsl import Text, Keyword, Index, Object, Integer, Mapping, Date
from strix.pipeline.mappingutil import annotation_analyzer, get_standard_analyzer, get_swedish_analyzer, similarity_tags_analyzer
from strix.config import config
import strix.pipeline.idgenerator as idgenerator
import strix.corpusconf as corpusconf
import elasticsearch


class CreateIndex:
    number_of_shards = config.number_of_shards
    number_of_replicas = config.number_of_replicas
    terms_number_of_shards = config.terms_number_of_shards
    terms_number_of_replicas = config.terms_number_of_replicas

    def __init__(self, index, reindexing=False):
        """
        :param index: name of index (alias name, date and time will be appended)
        :param reindexing: if true, terms-index will not be created (only necessary when creating index from scratch)
                            also no need for sequences
        """
        self.es = elasticsearch.Elasticsearch(config.elastic_hosts, timeout=120)

        corpus_config = corpusconf.get_corpus_conf(index)
        self.word_attributes = []
        for attr in corpus_config["analyze_config"]["word_attributes"]:
            self.word_attributes.append(attr)
        for nodeName, attributes in corpus_config["analyze_config"]["struct_attributes"].items():
            for attr in attributes:
                new_attr = dict(attr)
                new_attr["name"] = nodeName + "_" + attr["name"]
                self.word_attributes.append(new_attr)

        self.text_attributes = filter(lambda x: not x["ignore"] if "ignore" in x else True, corpus_config["analyze_config"]["text_attributes"])
        self.reindexing = reindexing
        self.alias = index

    def create_index(self):
        base_index, index_name = self.get_unique_index()
        base_index.create()
        self.es.cluster.health(index=index_name, wait_for_status="yellow")
        self.es.indices.close(index=index_name)
        self.create_text_type(index_name)
        self.es.indices.open(index=index_name)
        if not self.reindexing:
            self.create_term_position_index()
            idgenerator.create_sequence_index()
            idgenerator.reset_sequence(self.alias)
        return index_name

    def get_unique_index(self, suffix=""):
        index_name = self.alias + "_" + time.strftime("%Y%m%d-%H%M" + suffix)
        base_index = Index(index_name, using=self.es)
        if base_index.exists():
            return self.get_unique_index(suffix + "1" if suffix else "1")
        self.set_settings(base_index, CreateIndex.number_of_shards)
        return base_index, index_name

    def create_term_position_index(self):
        terms = Index(self.alias + "_terms", using=self.es)
        self.set_settings(terms, CreateIndex.terms_number_of_shards)
        terms.delete(ignore=404)
        terms.create()

        m = Mapping("term")
        m.meta("_all", enabled=False)
        m.meta("dynamic", "strict")
        m.meta("date_detection", False)
        m.meta("dynamic_templates", [
                {
                    "just_a_name": {
                        "path_match": "term.*",
                        "match_mapping_type": "string",
                        "mapping": {
                            "type": "keyword"
                        }
                    }
                }
            ])
        m.meta("_source", excludes=["text"])

        m.field("position", "integer")
        m.field("term", "object", dynamic=True)
        m.field("doc_id", "keyword", index="not_analyzed")
        m.field("doc_type", "keyword", index="not_analyzed")
        m.save(self.alias + "_terms", using=self.es)

    def set_settings(self, index, number_shards):
        index.settings(
            number_of_shards=number_shards,
            number_of_replicas=0,
            index={
                "unassigned": {
                    "node_left": {
                        "delayed_timeout": "1m"
                    }
                }
            }
        )

    def create_text_type(self, index_name):
        m = Mapping("text")
        m.meta("_all", enabled=False)
        m.meta("dynamic", "strict")

        text_field = Text(
            analyzer=get_standard_analyzer(),
            term_vector="with_positions_offsets",
            fields={
                "wid": Text(analyzer=annotation_analyzer("wid"), term_vector="with_positions_offsets"),
            }
        )

        for attr in self.word_attributes:
            annotation_name = attr["name"]
            text_field.fields[annotation_name] = Text(analyzer=annotation_analyzer(annotation_name, attr["set"]), term_vector="with_positions_offsets")

        m.field("text", text_field)

        for attr in self.text_attributes:
            if attr.get("type") == "date":
                mapping_type = Date(format="yyyyMMdd")
            elif attr.get("type") == "year":
                mapping_type = Integer()
            else:
                mapping_type = Keyword(index="not_analyzed")
            m.field(attr["name"], mapping_type)

        m.field("dump", Keyword(index=False, doc_values=False))
        m.field("lines", Object(enabled=False))
        m.field("word_count", Integer())
        m.field("similarity_tags", Text(analyzer=similarity_tags_analyzer()))

        title_field = Text(
            analyzer=get_swedish_analyzer(),
            fields={
                "raw": Keyword()
            }
        )
        m.field("title", title_field)
        m.field("original_file", Keyword())
        m.field("doc_id", Keyword())
        m.field("corpus_id", Keyword())

        m.save(index_name, using=self.es)

    def enable_insert_settings(self, index_name=None):
        self.es.indices.put_settings(index=(index_name or self.alias) + "," + self.alias + "_terms", body={
            "index.refresh_interval": -1,
        })

    def enable_postinsert_settings(self, index_name=None):
        self.es.indices.put_settings(index=index_name or self.alias, body={
            "index.number_of_replicas": CreateIndex.number_of_replicas,
            "index.refresh_interval": "30s"
        })
        self.es.indices.put_settings(index=self.alias + "_terms", body={
            "index.number_of_replicas": CreateIndex.terms_number_of_replicas,
            "index.refresh_interval": "30s"
        })
        self.es.indices.forcemerge(index=(index_name or self.alias) + "," + self.alias + "_terms")
