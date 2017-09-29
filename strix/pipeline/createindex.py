import time

from elasticsearch_dsl import Text, Keyword, Index, Object, Integer, Mapping, Date, GeoPoint, Nested, Double
import strix.pipeline.mappingutil as mappingutil
from strix.config import config
import strix.corpusconf as corpusconf
import elasticsearch


class CreateIndex:
    number_of_shards = config.number_of_shards
    number_of_replicas = config.number_of_replicas
    terms_number_of_shards = config.terms_number_of_shards
    terms_number_of_replicas = config.terms_number_of_replicas

    def __init__(self, index):
        """
        :param index: name of index (alias name, date and time will be appended)
        """
        self.es = elasticsearch.Elasticsearch(config.elastic_hosts, timeout=120)

        corpus_config = corpusconf.get_corpus_conf(index)
        self.word_attributes = []
        for attr_name in corpus_config["analyze_config"]["word_attributes"]:
            self.word_attributes.append(corpusconf.get_word_attribute(attr_name))
        self.fixed_structs = []
        for node_name, attributes in corpus_config["analyze_config"]["struct_attributes"].items():
            for attr_name in attributes:
                attr = corpusconf.get_struct_attribute(attr_name)
                if attr.get("index_in_text", True):
                    new_attr = dict(attr)
                    new_attr["name"] = node_name + "_" + attr["name"]
                    self.word_attributes.append(new_attr)
                else:
                    self.fixed_structs.append((node_name, attr))
        text_attributes = [corpusconf.get_text_attribute(attr_name) for attr_name in corpus_config["analyze_config"]["text_attributes"]]
        self.text_attributes = filter(lambda x: not x.get("ignore", False), text_attributes)
        self.alias = index

    def create_index(self):
        base_index, index_name = self.get_unique_index()
        base_index.create()
        self.es.cluster.health(index=index_name, wait_for_status="yellow")
        self.es.indices.close(index=index_name)
        self.create_text_type(index_name)
        self.es.indices.open(index=index_name)

        self.create_term_position_index()
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
                    "term_object_dynamic_template": {
                        "path_match": "term.*",
                        "match_mapping_type": "string",
                        "mapping": {
                            "type": "keyword"
                        }
                    }
                }
            ])

        m.field("position", "integer")

        fixed_props = {}
        for (node_name, attr) in self.fixed_structs:
            props = {}
            for prop_name, prop_value in attr["properties"].items():
                if prop_value["type"] == "geopoint":
                    props[prop_name] = GeoPoint()
                else:
                    props[prop_name] = Keyword()
            something = {attr["name"]: Nested(properties=props)}
            fixed_props[node_name] = Object(properties={"attrs": Object(properties=something)})

        m.field("term", "object", dynamic=True, properties={"attrs": Object("attrs", properties=fixed_props)})
        m.field("doc_id", "keyword", index="not_analyzed")
        m.field("doc_type", "keyword", index="not_analyzed")
        m.save(self.alias + "_terms", using=self.es)

    @staticmethod
    def set_settings(index, number_shards):
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
        m.meta("_source", excludes=["text"])

        text_field = Text(
            analyzer=mappingutil.get_token_annotation_analyzer(),
            fields={
                "wid": Text(analyzer=mappingutil.annotation_analyzer("wid")),
            }
        )

        for attr in self.word_attributes:
            annotation_name = attr["name"]
            if "ranked" in attr and attr["ranked"]:
                text_field.fields[annotation_name] = Text(analyzer=mappingutil.annotation_analyzer(annotation_name, is_set=False))
                annotation_name += "_alt"
                is_set = True
            else:
                is_set = attr.get("set", False)
            text_field.fields[annotation_name] = Text(analyzer=mappingutil.annotation_analyzer(annotation_name, is_set=is_set))

        m.field("text", text_field)

        for attr in self.text_attributes:
            if attr.get("ranked", False):
                mapping_type = Text(analyzer=mappingutil.ranked_text_analyzer(attr["name"]), fielddata=True)
            elif attr.get("type") == "date":
                mapping_type = Date(format="yyyyMMdd")
            elif attr.get("type") == "year":
                mapping_type = Integer()
            elif attr.get("type") == "double":
                mapping_type = Double(ignore_malformed=True)
            else:
                mapping_type = Keyword(index="not_analyzed")
            m.field(attr["name"], mapping_type)

        m.field("dump", Keyword(index=False, doc_values=False))
        m.field("lines", Object(enabled=False))
        m.field("word_count", Integer())
        m.field("similarity_tags", Text(analyzer=mappingutil.similarity_tags_analyzer(), term_vector="yes"))

        title_field = Text(
            analyzer=mappingutil.get_standard_analyzer(),
            fields={
                "raw": Keyword(),
                "analyzed": Text(analyzer=mappingutil.get_swedish_analyzer())
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
