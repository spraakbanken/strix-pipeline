import sys
import os
import unittest
import json
from elasticsearch_dsl.connections import connections

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../../')

import strix.api.web as web

connections.create_connection(hosts=["localhost"], timeout=120)


def check_corpus_content(corpus_id, last_config, current_config):
    assert "name" in current_config
    assert "description" in current_config
    assert "attributes" in current_config

    # word attributes
    last_word_attributes = {x["name"]: x for x in last_config["attributes"]["word_attributes"]}
    current_word_attributes = {x["name"]: x for x in current_config["attributes"]["word_attributes"]}

    for attr_name, attr_value in last_word_attributes.items():
        assert attr_name in current_word_attributes, "In current config, \"" + attr_name + "\" is not a word attribute for corpus \"" + corpus_id + "\""
    for attr_name, attr_value in current_word_attributes.items():
        assert attr_name in last_word_attributes, "In last config, \"" + attr_name + "\" was not a word attribute for corpus \"" + corpus_id + "\""

        for attr in ["name", "type", "set", "interval"]:
            msg = "Diff for word attribute \"" + attr_name + "\", field \"" + attr + "\", in corpus \"" + corpus_id + "\"."
            assert attr_value.get(attr, None) == last_word_attributes[attr_name].get(attr, None), msg

    # text attributes
    last_text_attributes = {x["name"]: x for x in last_config["attributes"]["text_attributes"]}
    current_text_attributes = {x["name"]: x for x in current_config["attributes"]["text_attributes"]}

    for attr_name, attr_value in last_text_attributes.items():
        assert attr_name in current_text_attributes, "In current config, \"" + attr_name + "\" is not a text attribute for corpus \"" + corpus_id + "\""
    for attr_name, attr_value in current_text_attributes.items():
        assert attr_name in last_text_attributes, "In last config, \"" + attr_name + "\" was not a text attribute for corpus \"" + corpus_id + "\""

        for attr in ["name", "type", "set", "interval", "include_in_aggregation", "aggs_interval", "has_infinite", "translation", "ignore"]:
            msg = "Diff for text attribute \"" + attr_name + "\", field \"" + attr + "\", in corpus \"" + corpus_id + "\"."
            assert attr_value.get(attr, None) == last_text_attributes[attr_name].get(attr, None), msg

    # struct attributes
    for struct_node in last_config["attributes"]["struct_attributes"].keys():
        assert struct_node in current_config["attributes"]["struct_attributes"], "In current config, \"" + struct_node + "\" is not a struct attribute for corpus \"" + corpus_id + "\""

    for struct_node in current_config["attributes"]["struct_attributes"].keys():
        assert struct_node in last_config["attributes"]["struct_attributes"], "In last config, \"" + struct_node + "\" was not a struct attribute for corpus \"" + corpus_id + "\""
        last_struct_attributes = {x["name"]: x for x in last_config["attributes"]["struct_attributes"][struct_node]}
        current_struct_attributes = {x["name"]: x for x in current_config["attributes"]["struct_attributes"][struct_node]}

        for attr_name, attr_value in last_struct_attributes.items():
            assert attr_name in current_struct_attributes, "In current config, \"" + struct_node + "." + attr_name + "\" is not a struct attribute for corpus \"" + corpus_id + "\""
        for attr_name, attr_value in current_struct_attributes.items():
            assert attr_name in last_struct_attributes, "In last config, \"" + struct_node + "." + attr_name + "\" was not a struct attribute for corpus \"" + corpus_id + "\""

            for attr in ["name", "type", "set", "interval", "index_in_text", "properties"]:
                msg = "Diff for struct attribute \"" + struct_node + "." + attr_name + "\", field \"" + attr + "\", in corpus \"" + corpus_id + "\"."
                assert attr_value.get(attr, None) == last_struct_attributes[attr_name].get(attr, None), msg



# Diff config against latest versions
class ConfigTest(unittest.TestCase):

    def setUp(self):
        self.app = web.app.test_client()

    def do_request(self, resource):
        rv = self.app.get(resource)
        return json.loads(rv.data.decode())

    def test_config(self):
        last_config = json.load(open("test_strix/data/config.json"))
        current_config = self.do_request("/config")

        assert sorted(list(current_config.keys())) == sorted(list(last_config.keys()))

        for corpus_id in current_config.keys():
            check_corpus_content(corpus_id, last_config[corpus_id], current_config[corpus_id])