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
        decode = rv.data.decode()
        return json.loads(decode)

    def test_strix_highlight_1(self):
        result = self.do_request("/search?text_query=framtid&corpora=vivill&exclude=dump,token_lookup,lines")
        for hit in result["data"]:
            assert "highlight" in hit
            for highlight in hit["highlight"]["highlight"]:
                assert len(highlight["match"]) == 1
                assert highlight["match"][0]["attrs"]["lemma"][0] == "framtid"
        assert result

    def test_strix_highlight_2(self):
        result = self.do_request("/search?text_query=Sveriges framtid&corpora=vivill&exclude=dump,token_lookup,lines")
        for hit in result["data"]:
            assert "highlight" in hit
            for highlight in hit["highlight"]["highlight"]:
                assert len(highlight["match"]) == 2
                assert highlight["match"][0]["attrs"]["lemma"][0] == "Sverige"
                assert highlight["match"][1]["attrs"]["lemma"][0] == "framtid"
        assert result

    # TODO this fails because one highlight is returned that do not contain an <em> tag??
    def test_simple_highlight(self):
        result = self.do_request("/search?text_query=framtid&corpora=vivill&exclude=dump,token_lookup,lines&simple_highlight=true")
        for hit in result["data"]:
            assert "highlight" in hit
            for highlight in hit["highlight"]["highlight"]:
                assert "<em>" in highlight
