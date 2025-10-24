import glob
import logging
import os

import strixpipeline.document.xmlparser as xmlparser

_logger = logging.getLogger(__name__)


def get_paths_for_corpus(config, corpus_id):
    conf = config.corpusconf.get_corpus_conf(corpus_id)
    corpus_dir_name = conf.get("corpus_dir") or conf.get("corpus_id")
    if config.texts_dir.startswith("/"):
        texts_dir = os.path.join(config.texts_dir, corpus_dir_name)
    else:
        texts_dir = os.path.join(config.base_dir, config.texts_dir, corpus_dir_name)
    return glob.glob(os.path.join(texts_dir, "**/*.xml")) + glob.glob(os.path.join(texts_dir, "*.xml"))


def process_work(config, corpus_conf, index, task_id, task):
    word_attrs = []
    pos_index = []
    for attr_name in corpus_conf["analyze_config"]["word_attributes"]:
        for attr_type, attr in attr_name.items():
            if type(attr) is str:
                attr = config.corpusconf.get_word_attribute(attr)
            # attr = config.corpusconf.get_word_attribute(attr_name)
            if attr.get("parse", True):
                word_attrs.append(attr)
            if attr.get("pos_index", False):
                pos_index.append(attr_type)
    word_annotations = {"token": word_attrs}

    struct_annotations = {}
    for node_name, attr_names in corpus_conf["analyze_config"]["struct_attributes"].items():
        structs = []
        for attr_name in attr_names:
            for attr_type, attr in attr_name.items():
                if type(attr) is str:
                    attr = config.corpusconf.get_struct_attribute(attr)
                if attr.get("parse", True):
                    structs.append(attr)
                if attr.get("pos_index", False):
                    # TODO this is probably the wrong name
                    pos_index.append(attr_type)
        struct_annotations[node_name] = structs

    text_attributes = {}
    remove_later = []
    for attr_name in corpus_conf["analyze_config"]["text_attributes"]:
        for attr_type, text_attribute in attr_name.items():
            if type(text_attribute) is str:
                text_attribute = config.corpusconf.get_struct_attribute(text_attribute)
            if text_attribute.get("parse", True):
                text_attributes[attr_type] = text_attribute
                if not text_attribute.get("save", True):
                    remove_later.append(attr_type)

    split_document = corpus_conf.get("split", "text")
    file_path = task["text"]
    text_tags = corpus_conf.get("text_tags")

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
    for text in texts:
        text["mode_id"] = corpus_conf["mode_id"]
        doc_id = text["text_attributes"]["_id"]
        text["doc_id"] = doc_id

        text["title"] = generate_title(corpus_conf, text)
        text["corpus_id"] = index
        text["original_file"] = file_name
        task = get_doc_task(index, text)
        task_terms = create_term_positions(index, doc_id, text["token_lookup"])
        del text["token_lookup"]
        for attribute in remove_later:
            if attribute in text["text_attributes"]:
                del text["text_attributes"][attribute]
        tasks.append(task)
        terms.extend(task_terms)

    return [tasks, terms]


def generate_title(corpus_conf, text):
    title_attr = corpus_conf.get("title")
    title = text["text_attributes"].get(title_attr)
    if title:
        return title
    else:
        return "N/A"


def get_doc_task(index, text):
    return {"_index": index, "_source": text}


def create_term_positions(index, text_id, token_lookup):
    terms = []
    for token in token_lookup:
        term = {
            "doc_id": text_id,
            "_index": index + "_terms",
            "_op_type": "index",
            "position": token["position"],
            "term": token,
        }
        terms.append(term)
    return terms
