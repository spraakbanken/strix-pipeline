import sys
import os
import unittest
import pytest
from elasticsearch_dsl.connections import connections

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../../')

import strix.api.elasticapi as api
from strix.api.elasticapihelpers import page_size

connections.create_connection(hosts=["localhost"], timeout=120)

class ElasticApiTest(unittest.TestCase):

    def test_empty_search(self):
        result = api.search("text", corpora="vivill", text_query={"text_query": "foobar"})
        assert result['data'] == []
        assert result['hits'] == 0

    def test_simple_search(self):
        result = api.search("text", corpora="vivill", text_query={"text_query": "hund"}, highlight={'number_of_fragments': 1})
        assert result['hits'] == 1
        item = result['data'][0]
        assert item["text_attributes"]['party'] != ''
        assert "hund" in item['highlight']['highlight'][0]['match'][0]['word']

    def test_simple_search_excludes(self):
        result = api.search("text", corpora="vivill", text_query={"text_query": "hund"}, excludes=['dump'])
        assert result['hits'] == 1
        item = result['data'][0]
        with pytest.raises(KeyError):
            item["text_attributes"]["dump"]

    def test_search_wrong_doc_type(self):
        doc_type = "asdf"
        result = api.search(doc_type, corpora="vivill", text_query={"text_query": "hund"})
        assert result['hits'] == 0

    def test_paging(self):
        result = api.search("text", corpora="vivill", size=page_size(25, 27))
        assert result['hits'] == 90
        assert len(result['data']) == 2

    def test_malformed_paging(self):
        with pytest.raises(RuntimeError):
            api.search("text", corpora="vivill", size=page_size(29, 27))

    def test_paging_zero_docs(self):
        result = api.search("text", corpora="vivill", size=page_size(to_hit=0))
        assert result['hits'] == 90
        assert len(result['data']) == 0

    def test_get_document(self):
        result = api.search("text", corpora="vivill", size=page_size(28, 29))
        item = result['data'][0]
        id = item['doc_id']
        result = api.get_document_by_id("vivill", "text", doc_id=id)
        assert result['data']

    def test_get_nonexistent_document(self):
        result = api.get_document_by_id("vivill", "text", doc_id="nonexisting")
        assert not result

    def test_search_in_document(self):
        doc_id = "c-2002v"
        result = api.search_in_document("vivill", "text", doc_id)
        assert result["doc_id"] == doc_id
