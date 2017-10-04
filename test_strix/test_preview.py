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

    def test_preview(self):
        result = self.do_request("/search?corpora=vivill&text_filter={\"party\": \"v\"}&simple_highlight=true")

        for hit in result["data"]:
            assert "preview" in hit
            # We want to have around 50 tokens
            assert len(hit["preview"].split(" ")) > 30



