import glob
import os
import logging
import json
import itertools
import strix.pipeline.xml_parser as xml_parser
import strix.config as config
import time


class InsertData:

    logger = logging.getLogger(__name__)

    def __init__(self, index):
        self.index = index
        self.corpus_conf = self.get_corpus_conf()

    def get_corpus_conf(self):
        return json.load(open("resources/config/" + self.index + ".json"))

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
                urls.append((text_id, "text", size, {"text": text}))
                print(text)
        return urls, tot_size

    def process(self, task_type, task_id, task_data, corpus_data):
        process_t = time.time()
        tasks = self.process_work(task_id, task_data, corpus_data)
        return tasks, time.time() - process_t

    def process_work(self, task_id, task, corpus_data):
        word_level_annotations = {
            "w":  self.corpus_conf["analyze_config"]["word_attributes"]
        }
        split_document = "text"
        file_name = task["text"]

        tasks = []
        terms = []
        for text in xml_parser.parse_pipeline_xml(file_name, split_document, word_level_annotations, set_text_attributes=True, token_count_id=True, generate_token_lookup=True):
            if self.corpus_conf["document_id"] == "task":
                doc_id = task_id
            else:
                doc_id = text[self.corpus_conf["document_id"]]
            task = self.get_doc_task(doc_id, "text", text)
            task_terms = self.create_term_positions(doc_id, text["token_lookup"])
            del text["token_lookup"]
            tasks.append(task)
            terms.extend(task_terms)

        return itertools.chain(tasks, terms or [])

    def get_doc_task(self, text_id, doc_type, text):
        if text_id.startswith("_"):
            InsertData.logger.warning("id starts with '_': %s" % text_id)
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
