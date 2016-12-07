import json
import os
import unittest

import pytest
import sys

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../../')

import strix.api.web as web


class WebApiTest(unittest.TestCase):

    corpus = "vivill"
    doc_ids = ["kd-2010v", "c-2002v", "c-1920v", "pp-2010v-kunskap", "v-1970v"]
    search = {"value": "alla familjer", "expected_results": 4}
    total_num_documents = 243
    # corpus = "wikipedia"
    # doc_ids = ["Julien Bahain", "Glomerate Creek (vattendrag i Kanada)", "Gimli (berg)", "Lundby kapell"]
    # search = {"value": "Havelock är huvudsakligen platt", "expected_results": 1}
    # total_num_documents = 17973

    host = "http://localhost:5000"
    corpus_config = json.load(open(os.path.dirname(os.path.realpath(__file__)) + "/../resources/config/" + corpus + ".json"))
    text_structures = ["dump", "lines", "es_id", "word_count", "title"]

    def setUp(self):
        self.app = web.app.test_client()

    def do_request(self, resource):
        rv = self.app.get(resource)
        return json.loads(rv.data.decode())
        # response = requests.get(WebApiTest.host + resource)
        # result = response.json()
        # return result

    def test_search(self):
        result = self.do_request("/search/" + WebApiTest.corpus + "/" + WebApiTest.search["value"] + "?exclude=text,dump,lines,token_lookup")
        assert result["hits"] == WebApiTest.search["expected_results"]

        for data in result["data"]:
            with pytest.raises(KeyError):
                data["dump"]
            with pytest.raises(KeyError):
                data["lines"]

        for token in result["data"][0]["highlight"]["highlight"][0]["match"]:
            assert token["word"] in WebApiTest.search["value"].split(" ")

    def test_get_document1(self):
        for doc_id in WebApiTest.doc_ids:
            result = self.do_request("/document/" + WebApiTest.corpus + "/" + doc_id + "?exclude=token_lookup")
            doc = result["data"]
            self.check_doc_text_attributes(doc)

    def test_get_document3(self):
        for doc_id in WebApiTest.doc_ids:
            result = self.do_request("/document/" + WebApiTest.corpus + "/" + doc_id + "?exclude=token_lookup")
            doc = result["data"]
            with pytest.raises(KeyError):
                doc["token_lookup"]

    # def test_get_document2(self):
    #     for doc_id in WebApiTest.doc_ids:
    #         result = self.do_request("/document/" + WebApiTest.corpus + "/" + doc_id + "?include=dump")
    #         doc = result['data']
    #         self.check_doc_text_attributes(doc)

    def test_get_documents(self):
        result = self.do_request("/document/" + WebApiTest.corpus + "/25/50?exclude=token_lookup")
        assert result["hits"] == WebApiTest.total_num_documents
        hits = result["data"]
        assert len(hits) == 25
        for doc in hits:
            self.check_doc_text_attributes(doc)

    def check_doc_text_attributes(self, doc):
        text_attributes = WebApiTest.corpus_config["analyze_config"]["text_attributes"]
        for text_attribute in text_attributes:
            assert doc[text_attribute]

        for text_attribute in WebApiTest.text_structures:
            assert doc[text_attribute]

        for text_attribute in doc.keys():
            assert text_attribute in text_attributes or text_attribute in WebApiTest.text_structures
