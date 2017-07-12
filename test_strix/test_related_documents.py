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

    def test_related_base(self):
        result = self.do_request("/related/vivill/m-1991v?exclude=text,dump,lines,token_lookup")
        assert result["hits"] == 87
        assert len(result["data"]) == 10

    def test_related_paging(self):
        result = self.do_request("/related/vivill/m-1991v?exclude=text,dump,lines,token_lookup&from=0&to=10")
        assert result["hits"] == 87
        assert len(result["data"]) == 10
        result = self.do_request("/related/vivill/m-1991v?exclude=text,dump,lines,token_lookup&from=80&to=90")
        assert result["hits"] == 87
        assert len(result["data"]) == 7

    def test_related_corpora_filter(self):
        result = self.do_request("/related/vivill/m-1991v?exclude=text,dump,lines,token_lookup&from=0&to=10&text_filter={\"corpus_id\": \"vivill\"}")
        assert result["hits"] == 48
        assert len(result["data"]) == 10
