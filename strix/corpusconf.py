import json
import os
import glob
import logging
from strix.config import config


_logger = logging.getLogger(__name__)


def get_corpus_conf(corpus_id):
    """
    get all information about corpus_id
    """
    return _all_config_files[corpus_id]


def get_text_attributes():
    """
    :return: a dict containing all text attributes by corpora
    """
    text_attributes = {}
    for key, conf in _all_config_files.items():
        try:
            text_attributes[key] = dict((attr["name"], attr) for attr in conf["analyze_config"]["text_attributes"])
        except KeyError:
            _logger.info("No text attributes for corpus: %s" % key)
            continue
        if "title" in text_attributes[key]:
            del text_attributes[key]["title"]

    # TODO WHY do we need this???
    text_attributes["litteraturbanken"] = []
    return text_attributes


def get_paths_for_corpus(corpus_id):
    conf = _all_config_files[corpus_id]
    corpus_dir_name = conf.get("corpus_dir") or conf.get("corpus_id")
    if config.texts_dir.startswith("/"):
        texts_dir = os.path.join(config.texts_dir, corpus_dir_name)
    else:
        texts_dir = os.path.join(config.base_dir, config.texts_dir, corpus_dir_name)
    return glob.glob(os.path.join(texts_dir, "**/*.xml")) + glob.glob(os.path.join(texts_dir, "*.xml"))


def _get_all_config_files():
    config_files = {}
    for file in glob.glob(_get_config_file("*")):
        key = os.path.splitext(os.path.basename(file))[0]
        config_files[key] = _fetch_corpus_conf(key)
    return config_files


def _fetch_corpus_conf(corpus_id):
    """
    Open requested corpus settings file and recursively fetch and merge
    with parent config file, if there is one.
    :param corpus_id: id of corpus to fetch
    :return: a dict containing configuration for corpus
    """
    config_file = _get_config_file(corpus_id)
    config_obj = json.load(open(config_file))
    if "parent" in config_obj:
        parent_obj = _fetch_corpus_conf(config_obj["parent"])
        _merge_configs(config_obj, parent_obj)
    return config_obj


def _get_config_file(corpus_id):
    return os.path.join(config.base_dir, "resources/config/" + corpus_id + ".json")


def _merge_configs(target, source):
    """
    Merge two corpus configurations.
    Moves attributes from source to target, so any definitions in both will
    be overwritten by source.
    :return: A new corpus configuration.
    """
    for k, v in source.items():
        if k in target:
            if k == "analyze_config":
                for k2, v2 in source[k].items():
                    if k2 == "text_attributes" or "word_attributes":
                        if k2 not in target[k]:
                            target[k][k2] = []
                        v2.extend(target[k][k2])
                        target[k][k2] = v2
                    elif k2 == "struct_attributes":
                        for k3, v3 in source[k][k2].items():
                            v3.extend(target[k][k2][k3])
                            target[k][k2][k3] = v3
                    else:
                        raise ValueError("Key: " + k + "." + k2 + ", not allowed in parent configuration.")
        else:
            target[k] = v

_all_config_files = _get_all_config_files()
