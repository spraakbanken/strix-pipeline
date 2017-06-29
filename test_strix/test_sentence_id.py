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

    def test_get_by_sentence1(self):
        result = self.do_request("/document/vivill/sentence/b3aaf83-b3ae169")
        assert result["data"]["doc_id"] == "m-1958v"

    def test_get_by_sentence2(self):
        result = self.do_request("/document/vivill/sentence/86e871f2-86e0cbe6")
        assert result["data"]["doc_id"] == "pp-2010v-integritet"
