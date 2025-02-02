import time
import logging

from elasticsearch_dsl import (
    Text,
    Keyword,
    Index,
    Object,
    Integer,
    Mapping,
    Date,
    Double,
    MetaField,
    InnerDoc,
    DenseVector,
)
import strixpipeline.mappingutil as mappingutil
from strixpipeline.config import config
import strixpipeline.elasticapi as elasticapi
import elasticsearch


_logger = logging.getLogger(__name__)


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
        w, t = self.set_attributes(index)
        self.word_attributes = w
        self.text_attributes = t
        self.alias = index

    def set_attributes(self, index):
        corpus_config = config.corpusconf.get_corpus_conf(index)
        word_attributes = []
        for attr_name in corpus_config["analyze_config"]["word_attributes"]:
            for attr_type, attr in attr_name.items():
                if type(attr) is str:
                    attr = config.corpusconf.get_word_attribute(attr)
                if attr.get("index", True):
                    # TODO map these in _terms index
                    pass
                if attr.get("pos_index", False):
                    word_attributes.append(attr)

        for node_name, attributes in corpus_config["analyze_config"]["struct_attributes"].items():
            for attr_name in attributes:
                for attr_type, attr in attr_name.items():
                    if type(attr) is str:
                        attr = config.corpusconf.get_struct_attribute(attr)
                    if attr.get("index", True):
                        # TODO map these in _terms index
                        pass
                    if attr.get("pos_index", False):
                        new_attr = dict(attr)
                        new_attr["name"] = node_name + "_" + attr["name"]
                        word_attributes.append(new_attr)

        text_attributes = []
        for attr_name in corpus_config["analyze_config"]["text_attributes"]:
            for attr_type, attr in attr_name.items():
                if type(attr) is str:
                    attr = config.corpusconf.get_struct_attribute(attr)
                if attr.get("index", True):
                    text_attributes.append(attr)

        return word_attributes, text_attributes

    def create_indices(self):
        base_index, index_name = self.get_unique_index()
        base_index.create()
        elasticapi.setup_alias(self.alias, index_name)
        elasticapi.close_index(index_name)
        self.create_text_type(index_name)
        elasticapi.open_index(index_name)

        term_index, term_index_name = self.get_unique_index(type="terms")
        term_index.create()
        elasticapi.setup_alias(self.alias + "_terms", term_index_name)
        self.create_term_position_index(term_index_name)

    def get_unique_index(self, type=None, suffix=""):
        index_name = self.alias + "_" + (type + "_" if type else "") + time.strftime("%Y%m%d-%H%M" + suffix)
        index = Index(index_name, using=self.es)
        if index.exists():
            return self.get_unique_index(type=type, suffix=suffix + "1" if suffix else "1")
        self.set_settings(index, CreateIndex.number_of_shards)
        return index, index_name

    def create_term_position_index(self, index_name):
        m = Mapping()
        m.meta("dynamic", "strict")
        m.meta("date_detection", False)
        m.meta(
            "dynamic_templates",
            [
                {
                    "term_object_dynamic_template": {
                        "path_match": "term.*",
                        "match_mapping_type": "string",
                        "mapping": {"type": "keyword"},
                    }
                }
            ],
        )

        m.field("position", "integer")

        m.field("term", Object(dynamic=True, properties={"attrs": Object()}))
        m.field("doc_id", "keyword")
        m.save(index_name, using=self.es)

    @staticmethod
    def set_settings(index, number_shards):
        index.settings(
            number_of_shards=number_shards,
            number_of_replicas=0,
            index={"unassigned": {"node_left": {"delayed_timeout": "1m"}}},
        )

    def create_text_type(self, index_name):
        m = Mapping()
        m.meta("dynamic", "strict")
        excludes = ["text", "wid", "sent_vector"]

        m.field("text", Text(analyzer=mappingutil.token_analyzer()))
        m.field("wid", Text(analyzer=mappingutil.annotation_analyzer()))

        for attr in self.word_attributes:
            annotation_name = attr["name"]
            excludes.append("pos_" + annotation_name)
            if attr.get("set", False):
                m.field(
                    "pos_" + annotation_name,
                    Text(analyzer=mappingutil.set_annotation_analyzer()),
                )
            else:
                m.field(
                    "pos_" + annotation_name,
                    Text(analyzer=mappingutil.annotation_analyzer()),
                )

        for attr in self.text_attributes:
            if attr.get("ranked", False):
                mapping_type = Text(
                    analyzer=mappingutil.ranked_text_analyzer(attr["name"]),
                    fielddata=True,
                )
            elif attr.get("type") == "date":
                mapping_type = Date(format="yyyyMMdd")
            elif attr.get("type") == "year":
                mapping_type = Integer()
            elif attr.get("type") == "double":
                mapping_type = Double(ignore_malformed=True)
            elif attr.get("type") == "integer":
                mapping_type = Integer()

            elif "properties" in attr:
                props = {}
                for prop_name, prop_val in attr["properties"].items():
                    props[prop_name] = Keyword()
                mapping_type = Object(properties=props)
            else:
                mapping_type = Keyword()
            m.field("text_" + attr["name"], mapping_type)
            excludes.append("text_" + attr["name"])

        m.meta("_source", excludes=excludes)

        m.field("text_attributes", Object(DisabledObject))
        m.field("dump", Keyword(index=False, doc_values=False))
        m.field("lines", Object(DisabledObject))
        m.field("word_count", Integer())
        m.field("most_common_words", Text())
        m.field("ner_tags", Text())
        m.field("geo_location", Keyword(multi=True))
        m.field("sent_vector", DenseVector(dims=768))

        # TODO: is the standard analyzer field used? otherwise move "analyzed" sub-field to top level
        title_field = Text(
            analyzer=mappingutil.get_standard_analyzer(),
            fields={
                "raw": Keyword(),
                "analyzed": Text(analyzer=mappingutil.get_swedish_analyzer()),
            },
        )
        m.field("title", title_field)
        m.field("original_file", Keyword())
        m.field("doc_id", Keyword())
        m.field("corpus_id", Keyword())
        m.field("mode_id", Keyword())

        m.save(index_name, using=self.es)

    def enable_insert_settings(self, index_name=None):
        # set refresh_interval to -1 to speed up indexing
        self.set_refresh_interval(index_name, -1)

    def enable_postinsert_settings(self, index_name=None):
        self.es.indices.put_settings(
            index=index_name or self.alias,
            body={
                "index.number_of_replicas": CreateIndex.number_of_replicas,
            },
        )

        self.es.indices.put_settings(
            index=self.alias + "_terms",
            body={
                "index.number_of_replicas": CreateIndex.terms_number_of_replicas,
            },
        )
        self.es.indices.forcemerge(index=(index_name or self.alias) + "," + self.alias + "_terms")
        self.set_refresh_interval(index_name, "1s")
        self.set_refresh_interval(index_name, -1)

    # TODO kräver faktiskt index namn??
    def set_refresh_interval(self, index_name, interval):
        self.es.indices.put_settings(
            index=(index_name or self.alias) + "," + self.alias + "_terms",
            body={
                "index.refresh_interval": interval,
            },
        )


class DisabledObject(InnerDoc):
    """
    Object(enabled=false) is not supported by DSL so this is needed to make objects disabled
    m.field("test", Object(DisabledObject))
    """

    class Meta:
        enabled = MetaField(False)


def create_index(corpus_id, delete_previous=False):
    if config.corpusconf.is_corpus(corpus_id):
        if delete_previous:
            elasticapi.delete_index_by_corpus_id(corpus_id)
        ci = CreateIndex(corpus_id)
        try:
            ci.create_indices()
        except elasticsearch.exceptions.TransportError as e:
            _logger.exception("transport error")
            raise e
    else:
        _logger.error('"' + corpus_id + '" is not a configured corpus')
