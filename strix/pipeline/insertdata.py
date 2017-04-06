import glob
import os
import logging
import json
import itertools
import strix.pipeline.xmlparser as xmlparser
from strix.config import config
import time

_logger = logging.getLogger(__name__)

class InsertData:

    def __init__(self, index):
        self.index = index
        self.corpus_conf = self.get_corpus_conf()

    def get_corpus_conf(self):
        return json.load(open(os.path.join(config.base_dir, "resources/config/" + self.index + ".json")))

    def prepare_urls(self, doc_ids):
        urls = []
        tot_size = 0
        texts_dir = os.path.join(config.texts_dir, self.corpus_conf.get("corpus_dir", self.corpus_conf["corpus_name"]))
        paths = glob.glob(os.path.join(texts_dir, "*.xml"))

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

    def process(self, task_type, task_id, task_data, corpus_data):
        process_t = time.time()
        tasks = self.process_work(task_id, task_data, corpus_data)
        return tasks, time.time() - process_t

    def process_work(self, task_id, task, corpus_data):
        word_annotations = {"w": self.corpus_conf["analyze_config"]["word_attributes"]}
        struct_annotations = self.corpus_conf["analyze_config"]["struct_attributes"]
        text_attributes = {}
        for text_attribute in self.corpus_conf["analyze_config"]["text_attributes"]:
            text_attributes[text_attribute["name"]] = text_attribute

        split_document = "text"
        file_name = task["text"]

        tasks = []
        terms = []
        for text in xmlparser.parse_pipeline_xml(file_name, split_document, word_annotations,
                                                 parser=self.corpus_conf.get("parser"),
                                                 struct_annotations=struct_annotations, text_attributes=text_attributes,
                                                 token_count_id=True, add_similarity_tags=True):
            if self.corpus_conf["document_id"] == "task":
                doc_id = task_id
            else:
                doc_id = text[self.corpus_conf["document_id"]]
            if "title" not in text:
                text["title"] = self.generate_title(text, text_attributes)
            text["original_file"] = os.path.basename(file_name)
            task = self.get_doc_task(doc_id, "text", text)
            task_terms = self.create_term_positions(doc_id, text["token_lookup"])
            del text["token_lookup"]
            tasks.append(task)
            terms.extend(task_terms)

        return itertools.chain(tasks, terms or [])

    def generate_title(self, text, text_attributes):
        title_keys = self.corpus_conf["title"]["keys"]
        format_params = {}
        for title_key in title_keys:
            if "translation" in text_attributes[title_key]:
                format_params[title_key] = text_attributes[title_key]["translation"][text[title_key]]
            else:
                format_params[title_key] = text[title_key]

        title_pattern = self.corpus_conf["title"]["pattern"]
        return title_pattern.format(**format_params)

    def get_doc_task(self, text_id, doc_type, text):
        if text_id.startswith("_"):
            _logger.warning("id starts with '_': %s" % text_id)
        return {
            "_index": self.index,
            "_type": doc_type,
            "_source": text,
            "_id": text_id
        }

    def create_term_positions(self, text_id, token_lookup):
        terms = []
        for token in token_lookup:
            term = {"doc_id": text_id,
                    "doc_type": "text",
                    "_index": self.index + "_terms",
                    "_type": "term",
                    "_op_type": "index",
                    "position": token["position"],
                    "term": token}
            terms.append(term)
        return terms
