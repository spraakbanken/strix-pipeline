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
        assert result["hits"] == 7
        for hit in result["data"]:
            for highlight in hit["highlight"]["highlight"]:
                assert highlight["match"][0]["word"].lower() == "bok"

    def test_only_quote(self):
        result = self.do_request("/search?exclude=dump,lines&text_query=\"")
        assert result["hits"] == 142
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
