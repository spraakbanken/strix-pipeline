import json
import os
import glob
from strix.config import config


def get_corpus_conf(corpus_id):
    config_file = get_config_file(corpus_id)
    config_obj = json.load(open(config_file))
    if "parent" in config_obj:
        parent_obj = get_corpus_conf(config_obj["parent"])
        merge_configs(config_obj, parent_obj)
    return config_obj


def get_text_attributes():
    text_attributes = {}
    for file in glob.glob(get_config_file("*")):
        key = os.path.splitext(os.path.basename(file))[0]
        conf = get_corpus_conf(key)
        try:
            text_attributes[key] = dict((attr["name"], attr) for attr in conf["analyze_config"]["text_attributes"])
        except:
            continue
        if "title" in text_attributes[key]:
            del text_attributes[key]["title"]

    text_attributes["litteraturbanken"] = []
    return text_attributes


def get_config_file(corpus_id):
    return os.path.join(config.base_dir, "resources/config/" + corpus_id + ".json")


def merge_configs(target, source):
    for k, v in source.items():
        if k in target:
            if k == "analyze_config":
                for k2, v2 in source[k].items():
                    if k2 == "text_attributes" or "word_attributes":
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
        else:
            target[k] = v
