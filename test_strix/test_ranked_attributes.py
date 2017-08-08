import json
import os
import unittest

import sys

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../../')

import strix.api.web as web


class WebApiTest(unittest.TestCase):

    def setUp(self):
        self.app = web.app.test_client()

    def do_request(self, resource):
        rv = self.app.get(resource)
        return json.loads(rv.data.decode())

    def test_search_top_ranked(self):
        result = self.do_request("http://localhost:5000/search?exclude=dump,lines,token_lookup,text_attributes&text_query=den..2&text_query_field=sense&highlight=false")
        assert result["hits"] == 1883

    def test_search_alternatives(self):
        result = self.do_request("http://localhost:5000/search?exclude=dump,lines,token_lookup,text_attributes&text_query=den..2&text_query_field=sense&highlight=false&include_alternatives")
        assert result["hits"] == 2692
