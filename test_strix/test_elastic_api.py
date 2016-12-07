import sys
import os
import unittest
import pytest
from elasticsearch_dsl.connections import connections

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../../')

import strix.api.elasticapi as api

connections.create_connection(hosts=["localhost"], timeout=120)

class ElasticApiTest(unittest.TestCase):

    def test_empty_search(self):
        result = api.search("vivill", "text", search_term="foobar")
        assert result['data'] == []
        assert result['hits'] == 0

    def test_simple_search(self):
        result = api.search("vivill", "text", search_term="hund", highlight={'number_of_fragments': 1})
        assert result['hits'] == 1
        item = result['data'][0]
        assert item['party'] != ''
        assert "hund" in item['highlight']['highlight'][0]['match'][0]['word']

    def test_simple_search_excludes(self):
        result = api.search("vivill", "text", search_term="hund", excludes=['dump'])
        assert result['hits'] == 1
        item = result['data'][0]
        with pytest.raises(KeyError):
            item['dump']

    def test_search_wrong_doc_type(self):
        doc_type = "asdf"
        result = api.search("vivill", doc_type, search_term="hund")
        assert result['hits'] == 0

    def test_paging(self):
        result = api.search("vivill", "text", from_hit=25, to_hit=27)
        assert result['hits'] == 243
        assert len(result['data']) == 2

    def test_malformed_paging(self):
        result = api.search("vivill", "text", from_hit=29, to_hit=27)
        assert len(result["data"]) == 0

    def test_get_document(self):
        result = api.search("vivill", "text", from_hit=28, to_hit=29)
        item = result['data'][0]
        id = item['es_id']
        result = api.get_document_by_id("vivill", "text", id, [], [])
        assert result['data']

    def test_get_nonexistent_document(self):
        result = api.get_document_by_id("vivill", "text", "nonexisting", [], [])
        assert not result
