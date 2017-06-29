import os
import logging
import itertools
import hashlib
import strix.pipeline.xmlparser as xmlparser
import time
import strix.pipeline.idgenerator as idgenerator
import strix.corpusconf as corpusconf

_logger = logging.getLogger(__name__)


class InsertData:

    def __init__(self, index):
        self.index = index
        self.corpus_conf = corpusconf.get_corpus_conf(self.index)

    def get_id_func(self, doc_count):
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
            def get_id_generator():
                ids = None
                while True:
                    if ids is None:
                        ids = idgenerator.get_id_sequence(self.index, doc_count)
                    try:
                        yield str(next(ids))
                    except StopIteration:
                        ids = None

            id_generator = get_id_generator()

            def generated_id(_, __):
                return next(id_generator)

            get_id = generated_id
        else:
            found = False
            for text_attr in self.corpus_conf["analyze_config"]["text_attributes"]:
                if text_attr["name"] == id_strategy:
                    found = True
            if not found:
                raise ValueError("\"" + id_strategy + "\" is not a text attribute, not possible to use for IDs")
            if "document_id_hash" in self.corpus_conf and self.corpus_conf["document_id_hash"]:
                def attribute_id(_, text):
                    m = hashlib.md5()
                    m.update(text[id_strategy].encode("utf-8"))
                    return str(int(m.hexdigest(), 16))[0:12]
            else:
                def attribute_id(_, text):
                    return text[id_strategy]
            get_id = attribute_id
        return get_id

    def prepare_urls(self, doc_ids):
        urls = []
        tot_size = 0
        paths = corpusconf.get_paths_for_corpus(self.index)

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
        word_annotations = {"w": self.corpus_conf["analyze_config"]["word_attributes"]}
        struct_annotations = self.corpus_conf["analyze_config"]["struct_attributes"]
        text_attributes = {}
        remove_later = []
        for text_attribute in self.corpus_conf["analyze_config"]["text_attributes"]:
            text_attributes[text_attribute["name"]] = text_attribute
            if "ignore" in text_attribute and text_attribute["ignore"]:
                remove_later.append(text_attribute["name"])

        split_document = "text"
        file_name = task["text"]

        texts = []
        for text in xmlparser.parse_pipeline_xml(file_name, split_document, word_annotations,
                                                 parser=self.corpus_conf.get("parser"),
                                                 struct_annotations=struct_annotations, text_attributes=text_attributes,
                                                 token_count_id=True, add_similarity_tags=True, save_whitespace_per_token=True):
            texts.append(text)

        tasks = []
        terms = []
        get_id = self.get_id_func(len(texts))
        for text in texts:
            doc_id = get_id(task_id, text)
            text["doc_id"] = doc_id
            self.generate_title(text, text_attributes)
            text["corpus_id"] = self.index
            text["original_file"] = os.path.basename(file_name)
            task = self.get_doc_task("text", text)
            task_terms = self.create_term_positions(doc_id, text["token_lookup"])
            del text["token_lookup"]
            for attribute in remove_later:
                del text[attribute]
            tasks.append(task)
            terms.extend(task_terms)

        return itertools.chain(tasks, terms or [])

    def generate_title(self, text, text_attributes):
        if "title" in self.corpus_conf:
            for setting in self.corpus_conf["title"]:
                if "title" in setting:
                    if setting["title"] in text:
                        text["title"] = text[setting["title"]]
                        break
                if "pattern" in setting:
                    title_keys = setting["keys"]
                    format_params = {}
                    for title_key in title_keys:
                        if title_key not in text:
                            return ""

                        if "translation" in text_attributes[title_key]:
                            format_params[title_key] = text_attributes[title_key]["translation"][text[title_key]]
                        else:
                            format_params[title_key] = text[title_key]

                    title_pattern = setting["pattern"]
                    text["title"] = title_pattern.format(**format_params)
                    break
            if "title" not in text:
                raise RuntimeError("Failed to set title for text")
        elif "title" not in text:
            raise RuntimeError("Configure \"title\" for corpus")

    def get_doc_task(self, doc_type, text):
        return {
            "_index": self.index,
            "_type": doc_type,
            "_source": text
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
