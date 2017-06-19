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
        result = self.do_request("/aggs")

        vivill_bucket = {}
        rdflista_bucket = {}
        for bucket in result["aggregations"]["corpora"]["buckets"]:
            if bucket["key"] == "vivill":
                vivill_bucket = bucket
            elif bucket["key"] == "rd-flista":
                rdflista_bucket = bucket

        # only aggregations and unused_facets should be sent back for this call
        assert vivill_bucket["doc_count"] == 243
        assert rdflista_bucket["doc_count"] == 1002
        result_keys = result.keys()
        assert len(result_keys) == 2
        assert "unused_facets" in result_keys

        # with no parameters supplied, aggs call give only corpora aggregation and the three most common other text-attributes aggs
        aggregation_keys = result["aggregations"].keys()
        assert len(aggregation_keys) == 4
        assert "subtitel" in aggregation_keys
        assert "organ" in aggregation_keys
        assert "datatyp" in aggregation_keys

    def test_rd_datatyp_bucket_no_filter(self):
        result = self.do_request("/aggs")
        keys = ["huvuddokument", "anforande", "forslag", "utskottsforslag"]
        buckets = {}
        for bucket in result["aggregations"]["datatyp"]["buckets"]:
            if bucket["key"] in keys:
                buckets[bucket["key"]] = bucket["doc_count"]

        for key in keys:
            assert key in buckets

        assert buckets["huvuddokument"] == 275916
        assert buckets["anforande"] == 91575
        assert buckets["forslag"] == 383135
        assert buckets["utskottsforslag"] == 6348

    def test_rd_datatyp_bucket_corpora_filter(self):
        """
        when we filter on the corpora rd-flista, rd-ip, rd-bet, we expect less results from each category
        """
        result = self.do_request("/aggs?corpora=rd-flista,rd-ip,rd-bet")
        keys = ["huvuddokument", "anforande", "forslag", "utskottsforslag"]
        buckets = {}
        for bucket in result["aggregations"]["datatyp"]["buckets"]:
            if bucket["key"] in keys:
                buckets[bucket["key"]] = bucket["doc_count"]
        assert buckets["huvuddokument"] == 27959
        assert buckets["anforande"] == 77153
        assert "forslag" not in buckets
        assert buckets["utskottsforslag"] == 6348

    def test_vivill_facets(self):
        """
        when we filter on the vivill corpora, we expect only vivill facets to appear (but still
        get document counts for each corpora)
        """
        result = self.do_request("/aggs?corpora=vivill")

        aggregation_keys = result["aggregations"].keys()
        assert len(aggregation_keys) == 4
        assert "type" in aggregation_keys
        assert "party" in aggregation_keys
        assert "year" in aggregation_keys

        rdflista_found = False
        wikipedia_found = False
        for corpus_bucket in result["aggregations"]["corpora"]["buckets"]:
            if corpus_bucket["key"] == "rd-flista":
                assert corpus_bucket["doc_count"] == 1002
                rdflista_found = True
            if corpus_bucket["key"] == "wikipedia":
                assert corpus_bucket["doc_count"] == 3454478
                wikipedia_found = True
        assert rdflista_found
        assert wikipedia_found

    def test_text_filter_1(self):
        result = self.do_request('/aggs?text_filter={"party": ["v","m"]}&corpora=vivill&')
        found_vivill = False
        for bucket in result["aggregations"]["corpora"]["buckets"]:
            if bucket["key"] == "vivill":
                assert bucket["doc_count"] == 74
                found_vivill = True
            else:
                assert False # no other corpora should support party and should therefore not appear in the list
        assert found_vivill

        # all parties should still appear in the list
        assert len(result["aggregations"]["party"]["buckets"]) == 22

        # but not all years, nor all types of documents
        year_count = 0
        for bucket in result["aggregations"]["year"]["buckets"]:
            if bucket["doc_count"] > 0:
                year_count += 1
        assert year_count == 44
        type_count = 0
        for bucket in result["aggregations"]["type"]["buckets"]:
            if bucket["doc_count"] > 0:
                type_count += 1
        assert type_count == 3

    def test_text_filter_2(self):
        result = self.do_request('/aggs?text_filter={"party": ["v","m"], "year": ["2010"]}&corpora=vivill')

        # only parties with documents from 2004 have non-zero values
        party_count = 0
        for bucket in result["aggregations"]["party"]["buckets"]:
            if bucket["doc_count"] > 0:
                party_count += 1
        assert party_count == 11
        # with years only constrained by "party"
        year_count = 0
        for bucket in result["aggregations"]["year"]["buckets"]:
            if bucket["doc_count"] > 0:
                year_count += 1
        assert year_count == 44
        # with type constrained by "party" and "year"
        type_count = 0
        for bucket in result["aggregations"]["type"]["buckets"]:
            if bucket["doc_count"] > 0:
                type_count += 1
        assert type_count == 1

    def test_expected_number_of_aggs_1(self):
        result = self.do_request("/aggs?facet_count=1")

        # with the field_count parameter supplied, aggs call give facet_count number of facets
        aggregation_keys = result["aggregations"].keys()
        assert len(aggregation_keys) == 1

    def test_expected_number_of_aggs_2(self):
        result = self.do_request("/aggs?facet_count=2")
        aggregation_keys = result["aggregations"].keys()
        assert len(aggregation_keys) == 2

    def test_exclude_empty_buckets(self):
        base_url = '/aggs?facet_count=3&corpora=vivill&text_filter={"party": ["v","m"]}'
        result = self.do_request(base_url)
        empty_bucket = False
        for v in result["aggregations"].values():
            buckets = v["buckets"]
            for bucket in buckets:
                if bucket["doc_count"] == 0:
                    empty_bucket = True
        assert empty_bucket

        result = self.do_request(base_url + "&exclude_empty_buckets")
        for v in result["aggregations"].values():
            buckets = v["buckets"]
            for bucket in buckets:
                assert bucket["doc_count"] > 0
