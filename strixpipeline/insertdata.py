import json
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

    def prepare_urls(self):
        urls = []
        tot_size = 0
        paths = get_paths_for_corpus(self.index)

        for text in paths:
            text_id = os.path.splitext(os.path.basename(text))[0]
            if os.path.isfile(text):
                f = open(text)
                size = os.fstat(f.fileno()).st_size
                tot_size += size
                urls.append(("text", text_id, size, {"text": text}))
                _logger.info(f"Adding file: {text}")
        return urls, tot_size

    def process(self, _, task_id, task_data):
        process_t = time.time()
        tasks = self.process_work(task_id, task_data)
        return tasks, time.time() - process_t

    def process_work(self, task_id, task):
        word_attrs = []
        pos_index = []
        for attr_name in self.corpus_conf["analyze_config"]["word_attributes"]:
            for attr_type, attr in attr_name.items():
                if type(attr) is str:
                    attr = config.corpusconf.get_word_attributeX(attr)
                # attr = config.corpusconf.get_word_attribute(attr_name)
                if attr.get("parse", True):
                    word_attrs.append(attr)
                if attr.get("pos_index", False):
                    pos_index.append(attr_type)
        word_annotations = {"token": word_attrs}

        struct_annotations = {}
        for node_name, attr_names in self.corpus_conf["analyze_config"]["struct_attributes"].items():
            structs = []
            for attr_name in attr_names:
                for attr_type, attr in attr_name.items():
                    if type(attr) is str:
                        attr = config.corpusconf.get_struct_attributeX(attr)
                    # attr = config.corpusconf.get_struct_attribute(attr_name)
                    if attr.get("parse", True):
                        structs.append(attr)
                    if attr.get("pos_index", False):
                        # TODO this is probably the wrong name
                        pos_index.append(attr_type)
            struct_annotations[node_name] = structs

        text_attributes = {}
        remove_later = []
        for attr_name in self.corpus_conf["analyze_config"]["text_attributes"]:
            for attr_type, text_attribute in attr_name.items():
                if type(text_attribute) is str:
                    text_attribute = config.corpusconf.get_text_attributeX(text_attribute)
                # text_attribute = config.corpusconf.get_text_attribute(attr_name)
                if text_attribute.get("parse", True):
                    text_attributes[attr_type] = text_attribute
                    if not text_attribute.get("save", True):
                        remove_later.append(attr_type)

        split_document = self.corpus_conf.get("split", "text")
        file_path = task["text"]
        text_tags = self.corpus_conf.get("text_tags")

        texts = []
        for text in xmlparser.parse_pipeline_xml(
            file_path,
            split_document,
            word_annotations,
            struct_annotations=struct_annotations,
            text_attributes=text_attributes,
            token_count_id=True,
            add_most_common_words=True,
            save_whitespace_per_token=True,
            pos_index_attributes=pos_index,
            text_tags=text_tags,
        ):
            texts.append(text)

        tasks = []
        terms = []

        file_name = os.path.basename(file_path)
        transformer_input = []
        for text in texts:
            text["mode_id"] = self.corpus_conf["mode_id"]
            doc_id = text["text_attributes"]["_id"]
            text["doc_id"] = doc_id
            self.generate_title(text, text_attributes)
            text["corpus_id"] = self.index
            text["original_file"] = file_name
            task = self.get_doc_task(text)
            task_terms = self.create_term_positions(doc_id, text["token_lookup"])
            del text["token_lookup"]
            for attribute in remove_later:
                if attribute in text["text_attributes"]:
                    del text["text_attributes"][attribute]
            tasks.append(task)
            terms.extend(task_terms)
            transformer_input.append([doc_id, " ".join(text["dump"]).replace("\n", "")])

        with open(
            os.path.join(
                config.transformers_postprocess_dir,
                self.index,
                f"texts/{file_name}.jsonl",
            ),
            "w",
        ) as fp:
            for text in transformer_input:
                fp.write(f"{json.dumps(text, ensure_ascii=False)}\n")

        return itertools.chain(tasks, terms or [])

    def generate_title(self, text, text_attributes):
        if self.corpus_conf["title"] == "n/a":
            text["title"] = "N/A"
        else:
            title = text["text_attributes"].get(self.corpus_conf["title"])
            if title:
                text["title"] = title
            else:
                text["title"] = "Title missing"

        if "title" not in text:
            raise RuntimeError('Configure "title" for corpus')

    def get_doc_task(self, text):
        return {"_index": self.index, "_source": text}

    def create_term_positions(self, text_id, token_lookup):
        terms = []
        for token in token_lookup:
            term = {
                "doc_id": text_id,
                "_index": self.index + "_terms",
                "_op_type": "index",
                "position": token["position"],
                "term": token,
            }
            terms.append(term)
        return terms
