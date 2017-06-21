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
    text_structures = ["dump", "lines", "doc_id", "word_count", "title", "text_attributes", "corpus", "corpus_id"]

    def setUp(self):
        self.app = web.app.test_client()

    def do_request(self, resource):
        rv = self.app.get(resource)
        return json.loads(rv.data.decode())

    def test_search(self):
        result = self.do_request("/search?exclude=text,dump,lines,token_lookup&corpora=" + WebApiTest.corpus + "&text_query=" + WebApiTest.search["value"])
        assert result["hits"] == WebApiTest.search["expected_results"]

        for token in result["data"][0]["highlight"]["highlight"][0]["match"]:
            assert token["word"] in WebApiTest.search["value"].split(" ")

        for data in result["data"]:
            with pytest.raises(KeyError):
                data["dump"]
            with pytest.raises(KeyError):
                data["lines"]

            data["dump"] = "tmp"
            data["lines"] = "tmp"
            assert data["doc_type"]
            del data["doc_type"]
            del data["highlight"]
            self.check_doc_text_attributes(data)

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

    def test_get_document_with_bad_doc_id(self):
        result = self.do_request("/document/" + WebApiTest.corpus + "/qwerty?exclude=token_lookup")
        assert result == {}

    def test_get_documents(self):
        result = self.do_request("/search?exclude=token_lookup&from=25&to=50&corpora=" + WebApiTest.corpus)
        assert result["hits"] == WebApiTest.total_num_documents
        hits = result["data"]
        assert len(hits) == 25
        for doc in hits:
            assert doc["doc_type"]
            del doc["doc_type"]
            self.check_doc_text_attributes(doc)

    def test_filters(self):
        result = self.do_request('/search?exclude=token_lookup&from=25&to=50&text_filter={"party": "m"}&corpora=' + WebApiTest.corpus)
        assert result["hits"] == 39

    def test_mutli_word_search(self):
        # when one word does not have a lemmatization s.a. missspelled words we want the search not to fail
        result = self.do_request("/search?corpora=vivill&from=0&to0&text_query=sverges framtid")
        assert result["hits"] == 0

    def check_doc_text_attributes(self, doc):
        text_attributes = [obj["name"] for obj in WebApiTest.corpus_config["analyze_config"]["text_attributes"]]
        for text_attribute in text_attributes:
            assert doc["text_attributes"][text_attribute]

        for text_attribute in WebApiTest.text_structures:
            assert doc[text_attribute]

        for text_field in doc.keys():
            assert text_field in WebApiTest.text_structures

        for text_attribute in doc["text_attributes"].keys():
            assert text_attribute in text_attributes