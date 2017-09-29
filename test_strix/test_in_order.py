import json
import os
import unittest

import pytest
import sys

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../../')

import strix.api.web as web

class WebApiTest(unittest.TestCase):

    host = "http://localhost:5000"

    def setUp(self):
        self.app = web.app.test_client()

    def do_request(self, resource):
        rv = self.app.get(resource)
        return json.loads(rv.data.decode())

    def test_in_order(self):
        result = self.do_request("/search?exclude=text,dump,lines,token_lookup&text_query=storm el")
        assert result["hits"] == 0

    def test_not_in_order(self):
        result = self.do_request("/search?exclude=text,dump,lines,token_lookup&text_query=storm el&in_order=false")
        assert result["hits"] == 12

        for hit in result["data"]:
            in_text = "highlight" in hit and len(hit["highlight"]["highlight"]) > 0
            in_title = "storm" in hit["title"].lower() or "el" in hit["title"].lower()
            assert (in_text or in_title)

            if in_text:
                for highlight in hit["highlight"]["highlight"]:
                    for match in highlight["match"]:
                        word = match["word"].lower()
                        assert word in ["el", "elen", "stormen", "stormar", "storm", "stormarna"]
