import json
import os
import unittest
import pytest

import sys

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../')

import strix.api.web as web


class GeocontextTest(unittest.TestCase):

    def setUp(self):
        self.app = web.app.test_client()

    def do_request(self, resource):
        rv = self.app.get(resource)
        return json.loads(rv.data.decode())

    # TODO implement this
    def test_aggs(self):
        with pytest.raises(Exception):
            result = self.do_request("/aggs/wikipedia/221453884131/sentence.geocontext")

    def test_search_in_document(self):
        pass

    def test_search(self):
        pass
