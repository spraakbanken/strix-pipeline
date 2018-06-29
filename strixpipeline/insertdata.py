import os
import logging
import itertools
import hashlib
import time
import uuid
import glob
import strixpipeline.xmlparser as xmlparser
from strixpipeline.config import config

_logger = logging.getLogger(__name__)


def get_paths_for_corpus(corpus_id):
    conf = config.corpusconf.get_corpus_conf(corpus_id)
    corpus_dir_name = conf.get("corpus_dir") or conf.get("corpus_id")
    if config.texts_dir.startswith("/"):
        texts_dir = os.path.join(config.texts_dir, corpus_dir_name)
    else:
        texts_dir = os.path.join(config.base_dir, config.texts_dir, corpus_dir_name)
    return glob.glob(os.path.join(texts_dir, "**/*.xml")) + glob.glob(os.path.join(texts_dir, "*.xml"))


class InsertData:

    def __init__(self, index):
        self.index = index
        self.corpus_conf = config.corpusconf.get_corpus_conf(self.index)

    def get_id_func(self):
        """
        the supported strategies for "document_id" are:
        - "filename" - use the filename / task id. Each file must contain only
          one document for this to work.
        - "generated" - generate a new id for each document, this will be removed when
          all texts have IDs
        - attribute name - Use an attribute in the document s.a. "title" or "_id"
          The attribute must be a configured text-attribute, but can be ignored for insertion.
        """
        id_strategy = self.corpus_conf["document_id"]
        if id_strategy == "filename":
            def task_id_fun(task_id, _):
                return task_id
            get_id = task_id_fun
        elif id_strategy == "generated":
            def generated_id(_, __):
                return uuid.uuid4()
            get_id = generated_id
        else:
            found = False
            for text_attr in self.corpus_conf["analyze_config"]["text_attributes"]:
                if text_attr == id_strategy:
                    found = True
            if not found:
                raise ValueError("\"" + id_strategy + "\" is not a text attribute, not possible to use for IDs")
            if "document_id_hash" in self.corpus_conf and self.corpus_conf["document_id_hash"]:
                def attribute_id(_, text):
                    m = hashlib.md5()
                    m.update(text["text_attributes"][id_strategy].encode("utf-8"))
                    return str(int(m.hexdigest(), 16))[0:12]
            else:
                def attribute_id(_, text):
                    return text["text_attributes"][id_strategy]
            get_id = attribute_id
        return get_id

    def prepare_urls(self, doc_ids):
        urls = []
        tot_size = 0
        paths = get_paths_for_corpus(self.index)

        for text in paths:
            text_id = os.path.splitext(os.path.basename(text))[0]
            include_doc = not doc_ids or text_id in doc_ids
            if include_doc and os.path.isfile(text):
                f = open(text)
                size = os.fstat(f.fileno()).st_size
                tot_size += size
                urls.append(("text", text_id, size, {"text": text}))
                _logger.info(text)
        return urls, tot_size

    def process(self, _, task_id, task_data, corpus_data):
        process_t = time.time()
        tasks = self.process_work(task_id, task_data, corpus_data)
        return tasks, time.time() - process_t

    def process_work(self, task_id, task, _):
        word_attrs = []
        pos_index = []
        for attr_name in self.corpus_conf["analyze_config"]["word_attributes"]:
            attr = config.corpusconf.get_word_attribute(attr_name)
            if attr.get("parse", True):
                word_attrs.append(attr)
            if attr.get("posIndex", False):
                pos_index.append(attr_name)
        word_annotations = {"w": word_attrs}

        struct_annotations = {}
        for node_name, attr_names in self.corpus_conf["analyze_config"]["struct_attributes"].items():
            structs = []
            for attr_name in attr_names:
                attr = config.corpusconf.get_struct_attribute(attr_name)
                if attr.get("parse", True):
                    structs.append(attr)
                if attr.get("posIndex", False):
                    # TODO this is probably the wrong name
                    pos_index.append(attr_name)
            struct_annotations[node_name] = structs

        text_attributes = {}
        remove_later = []
        for attr_name in self.corpus_conf["analyze_config"]["text_attributes"]:
            text_attribute = config.corpusconf.get_text_attribute(attr_name)
            if text_attribute.get("parse", True):
                text_attributes[attr_name] = text_attribute
                if not text_attribute.get("save", True):
                    remove_later.append(attr_name)

        plugin = None
        pipeline_plugin = self.corpus_conf.get("pipeline_plugin")
        if pipeline_plugin:
            plugin = config.corpusconf.get_plugin(pipeline_plugin)

        split_document = "text"
        file_name = task["text"]

        texts = []
        for text in xmlparser.parse_pipeline_xml(file_name, split_document, word_annotations,
                                                 struct_annotations=struct_annotations, text_attributes=text_attributes,
                                                 token_count_id=True, add_similarity_tags=True, save_whitespace_per_token=True,
                                                 plugin=plugin, pos_index_attributes=pos_index):
            texts.append(text)

        tasks = []
        terms = []
        get_id = self.get_id_func()
        for text in texts:
            doc_id = get_id(task_id, text)
            text["doc_id"] = doc_id
            self.generate_title(text, text_attributes)
            text["corpus_id"] = self.index
            text["original_file"] = os.path.basename(file_name)
            task = self.get_doc_task(text)
            task_terms = self.create_term_positions(doc_id, text["token_lookup"])
            del text["token_lookup"]
            for attribute in remove_later:
                if attribute in text["text_attributes"]:
                    del text["text_attributes"][attribute]
            tasks.append(task)
            terms.extend(task_terms)

        return itertools.chain(tasks, terms or [])

    def generate_title(self, text, text_attributes):
        if "title" in self.corpus_conf:
            for setting in self.corpus_conf["title"]:
                if "title" in setting:
                    if "text_" + setting["title"] in text:
                        text["title"] = text["text_attributes"][setting["title"]]
                        break
                if "pattern" in setting:
                    title_keys = setting["keys"]
                    format_params = {}
                    try:
                        for title_key in title_keys:
                            if "translation_value" in text_attributes[title_key]:
                                attr = text_attributes[title_key]["translation_value"][text["text_attributes"][title_key]]
                                format_params[title_key] = attr.get("-") or attr.get("swe")
                            else:
                                format_params[title_key] = text["text_attributes"][title_key]

                        title_pattern = setting["pattern"]
                        text["title"] = title_pattern.format(**format_params)
                        break
                    except KeyError:
                        # if something breaks, just try the next setting for title
                        pass

            if "title" not in text:
                raise RuntimeError("Failed to set title for text")
        else:
            title = text["text_attributes"].get("title")
            if title:
                text["title"] = title

        if "title" not in text:
            raise RuntimeError("Configure \"title\" for corpus")

    def get_doc_task(self, text):
        return {
            "_index": self.index,
            "_type": "doc",
            "_source": text
        }

    def create_term_positions(self, text_id, token_lookup):
        terms = []
        for token in token_lookup:
            term = {
                "doc_id": text_id,
                "doc_type": "text",
                "_index": self.index + "_terms",
                "_type": "doc",
                "_op_type": "index",
                "position": token["position"],
                "pos_str": str(token["position"]),
                "term": token
            }
            terms.append(term)
        return terms
