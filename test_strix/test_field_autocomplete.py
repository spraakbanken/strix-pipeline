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

    def test_initial_request(self):
        result = self.do_request("/aggs/vivill/m-1991v/lemgram")
        assert result
        assert result["aggregations"]["lemgram"]
        assert len(result["aggregations"]["lemgram"]["buckets"]) == 704
        found1 = False
        found2 = False
        for bucket in result["aggregations"]["lemgram"]["buckets"]:
            if bucket["key"] == "en..al.1":
                found1 = True
                assert bucket["doc_count"] == 100
            if bucket["key"] == "Europa..pm.2":
                found2 = True
                assert bucket["doc_count"] == 10
        assert found1
        assert found2

    def test_structural_attributes(self):
        result = self.do_request("/aggs/wikipedia/231411843919/ne.name")
        assert "ne.name" in result["aggregations"]
        found = False
        for bucket in result["aggregations"]["ne.name"]["buckets"]:
            if bucket["key"] == "Under andra hälften av 1300-talet":
                found = True
                assert bucket["doc_count"] == 1
        assert found