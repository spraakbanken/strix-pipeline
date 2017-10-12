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


def get_word_attribute(attr_name):
    return _word_attributes[attr_name]


def get_struct_attribute(attr_name):
    return _struct_attributes[attr_name]


def get_text_attribute(attr_name):
    return _text_attributes[attr_name]


def get_text_attributes():
    """
    :return: a dict containing all text attributes by corpora
    """
    text_attributes = {}
    for key, conf in _all_config_files.items():
        try:
            text_attributes[key] = dict((attr, _text_attributes[attr]) for attr in conf["analyze_config"]["text_attributes"])
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


def is_ranked(word_attribute):
    try:
        return _word_attributes[word_attribute].get("ranked", False)
    except KeyError:
        raise ValueError("\"" + word_attribute + "\" is not configured")


def is_object(path):
    try:
        if path[-1] in _struct_attributes:
            return not _struct_attributes[path[-1]].get("index_in_text", True)
        return False
    except KeyError:
        raise ValueError("\"" + ".".join(path) + "\" is not configured")


def get_type_info():
    return _type_info


def _load_type_info():
    type_file = os.path.join(config.base_dir, "resources/config/attributes/types.json")
    return json.load(open(type_file))


def _get_all_config_files():
    config_files = {}
    for file in glob.glob(_get_config_file("*")):
        key = os.path.splitext(os.path.basename(file))[0]
        config_files[key] = _fetch_corpus_conf(key)
    return config_files


def _fetch_corpus_conf(corpus_id, config_type="corpora"):
    """
    Open requested corpus settings file and recursively fetch and merge
    with parent config file, if there is one.
    :param corpus_id: id of corpus to fetch
    :return: a dict containing configuration for corpus
    """
    config_file = _get_config_file(corpus_id, config_type)
    try:
        config_obj = json.load(open(config_file))
    except:
        _logger.error("Could not read config file: " + config_file)
        raise

    parents = config_obj.get("parents", [])
    for parent in parents:
        parent_obj = _fetch_corpus_conf(parent, config_type="corpora_templates")
        _merge_configs(config_obj, parent_obj)
    return config_obj


def _get_config_file(corpus_id, config_type="corpora"):
    return os.path.join(config.base_dir, "resources/config", config_type, corpus_id + ".json")


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
                for k2, v2 in source["analyze_config"].items():
                    if k2 in ["text_attributes", "word_attributes"]:
                        if k2 not in target["analyze_config"]:
                            target["analyze_config"][k2] = []
                        v2.extend(target["analyze_config"][k2])
                        target["analyze_config"][k2] = v2
                    elif k2 == "struct_attributes":
                        for k3, v3 in source["analyze_config"]["struct_attributes"].items():
                            if "struct_attributes" not in target["analyze_config"]:
                                target["analyze_config"]["struct_attributes"] = {}
                            elif k3 in target["analyze_config"]["struct_attributes"]:
                                v3.extend(target["analyze_config"]["struct_attributes"][k3])
                            target["analyze_config"]["struct_attributes"][k3] = v3
                    else:
                        raise ValueError("Key: " + k + "." + k2 + ", not allowed in parent configuration.")
        else:
            target[k] = v


def _get_attributes(attr_type):
    return json.load(open(os.path.join(config.base_dir, "resources/config/attributes", attr_type + ".json")))


_all_config_files = _get_all_config_files()
_word_attributes = _get_attributes("word_attributes")
_struct_attributes = _get_attributes("struct_attributes")
_text_attributes = _get_attributes("text_attributes")
_type_info = _load_type_info()
