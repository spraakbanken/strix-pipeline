import json
import os
import unittest

import sys

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../')

import strix.api.web as web


class FacetetSearchTest(unittest.TestCase):

    def setUp(self):
        self.app = web.app.test_client()

    def do_request(self, resource):
        rv = self.app.get(resource)
        return json.loads(rv.data.decode())

    def test_quote_search(self):
        result = self.do_request("/search?exclude=dump,lines&text_query=\"bok\"")
        assert result["hits"] == 8
        for hit in result["data"]:
            for highlight in hit["highlight"]["highlight"]:
                assert highlight["match"][0]["word"].lower() == "bok"

    def test_only_quote(self):
        result = self.do_request("/search?exclude=dump,lines&text_query=\"")
        assert result["hits"] == 146
        for hit in result["data"]:
            for highlight in hit["highlight"]["highlight"]:
                assert highlight["match"][0]["word"] == '"'

    def test_unlemmatizable(self):
        result = self.do_request("/search?exclude=dump,lines&text_query=null")
        assert result["hits"] == 0
        assert len(result["data"]) == 0

    def test_multi_word_quote_search(self):
        result = self.do_request("/search?exclude=dump,lines&text_query=\"var en svensk\"")
        assert result["hits"] == 31
        for hit in result["data"]:
            for highlight in hit["highlight"]["highlight"]:
                assert highlight["match"][0]["word"].lower() == "var"
                assert highlight["match"][1]["word"].lower() == "en"
                assert highlight["match"][2]["word"].lower() == "svensk"

    def test_multi_word_quote_search2(self):
        result = self.do_request("/search?exclude=dump,lines&text_query=\"det är en\"&corpora=vivill")
        assert result["hits"] == 23
        for hit in result["data"]:
            for highlight in hit["highlight"]["highlight"]:
                assert highlight["match"][0]["word"].lower() == "det"
                assert highlight["match"][1]["word"].lower() == "är"
                assert highlight["match"][2]["word"].lower() == "en"

    def test_multi_word_quote_search3(self):
        result = self.do_request("/search?exclude=dump,lines&text_query=\"det är\" en&corpora=vivill")
        assert result["hits"] == 31
        found_de = False
        found_en = False
        found_den = False
        found_ett = False
        for hit in result["data"]:
            for highlight in hit["highlight"]["highlight"]:
                assert highlight["match"][0]["word"].lower() == "det"
                assert highlight["match"][1]["word"].lower() == "är"
                word = highlight["match"][2]["word"].lower()
                if word == "de":
                    found_de = True
                elif word == "den":
                    found_den = True
                elif word == "ett":
                    found_ett = True
                elif word == "en":
                    found_en = True

        assert found_de
        assert found_den
        assert found_ett
        assert found_en

    def test_search_corpus_id1(self):
        result = self.do_request("/search?exclude=dump,lines&text_query=\"det är en\"&text_filter={\"corpus_id\": \"vivill\"}")
        assert result["hits"] == 23

    def test_search_corpus_id2(self):
        result = self.do_request("/search?exclude=dump,lines&text_query=\"det är en\"&text_filter={\"corpus_id\": [\"rd-sou\", \"vivill\"]}")
        assert result["hits"] == 25
