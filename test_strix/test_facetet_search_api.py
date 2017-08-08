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
        for bucket in result["aggregations"]["corpus_id"]["buckets"]:
            if bucket["key"] == "vivill":
                vivill_bucket = bucket
            elif bucket["key"] == "rd-flista":
                rdflista_bucket = bucket

        # only aggregations and unused_facets should be sent back for this call
        assert vivill_bucket["doc_count"] == 90
        assert rdflista_bucket["doc_count"] == 632
        result_keys = result.keys()
        assert len(result_keys) == 2
        assert "unused_facets" in result_keys

        # with no parameters supplied, aggs call give only corpora aggregation and the three most common other text-attributes aggs
        aggregation_keys = result["aggregations"].keys()
        assert len(aggregation_keys) == 4
        assert "datatyp" in aggregation_keys

    def test_rd_datatyp_bucket_no_filter(self):
        result = self.do_request("/aggs")
        keys = ["huvuddokument", "anforande"]
        buckets = {}
        for bucket in result["aggregations"]["datatyp"]["buckets"]:
            if bucket["key"] in keys:
                buckets[bucket["key"]] = bucket["doc_count"]

        for key in keys:
            assert key in buckets

        assert buckets["huvuddokument"] == 1136
        assert buckets["anforande"] == 1518

    def test_rd_datatyp_bucket_corpora_filter(self):
        """
        when we filter on the corpora rd-flista, rd-kammakt we expect less results from each category
        """
        result = self.do_request("/aggs?corpora=rd-flista,rd-kammakt")
        keys = ["huvuddokument", "anforande", "forslag", "utskottsforslag"]
        buckets = {}
        for bucket in result["aggregations"]["datatyp"]["buckets"]:
            if bucket["key"] in keys:
                buckets[bucket["key"]] = bucket["doc_count"]
        assert buckets["huvuddokument"] == 811
        assert buckets["anforande"] == 1518
        assert "forslag" not in buckets

    def test_vivill_facets(self):
        """
        when we filter on the vivill corpora, we expect only vivill facets to appear (but still
        get document counts for each corpora)
        """
        result = self.do_request("/aggs?corpora=vivill&facet_count=6")

        aggregation_keys = result["aggregations"].keys()
        assert len(aggregation_keys) == 6
        assert "type" in aggregation_keys
        assert "party" in aggregation_keys
        assert "year" in aggregation_keys

        rdflista_found = False
        wikipedia_found = False
        for corpus_bucket in result["aggregations"]["corpus_id"]["buckets"]:
            if corpus_bucket["key"] == "rd-flista":
                assert corpus_bucket["doc_count"] == 632
                rdflista_found = True
            if corpus_bucket["key"] == "wikipedia":
                assert corpus_bucket["doc_count"] == 172
                wikipedia_found = True
        assert rdflista_found
        assert wikipedia_found

    def test_text_filter_1(self):
        result = self.do_request('/aggs?text_filter={"party": ["v","m"]}&corpora=vivill&include_facets=year,type,party')
        found_vivill = False
        for bucket in result["aggregations"]["corpus_id"]["buckets"]:
            if bucket["key"] == "vivill":
                assert bucket["doc_count"] == 23
                found_vivill = True
            else:
                assert False # no other corpora should support party and should therefore not appear in the list
        assert found_vivill

        # all parties should still appear in the list
        assert len(result["aggregations"]["party"]["buckets"]) == 20

        # but not all years, nor all types of documents
        year_count = 0
        for bucket in result["aggregations"]["year"]["buckets"]:
            if bucket["doc_count"] > 0:
                year_count += 1
        assert year_count == 13
        type_count = 0
        for bucket in result["aggregations"]["type"]["buckets"]:
            if bucket["doc_count"] > 0:
                type_count += 1
        assert type_count == 1

    def test_text_filter_2(self):
        result = self.do_request('/aggs?text_filter={"party": ["v","m"], "year": ["2010"]}&corpora=vivill&include_facets=year,type,party')

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
        assert year_count == 13
        # with type constrained by "party" and "year"
        type_count = 0
        for bucket in result["aggregations"]["type"]["buckets"]:
            if bucket["doc_count"] > 0:
                type_count += 1
        assert type_count == 1

    def test_text_filter_3(self):
        result = self.do_request('/aggs?text_filter={"party": ["v","m"]}&facet_count=6')
        aggregation_keys = result["aggregations"].keys()
        assert len(aggregation_keys) == 6
        assert "type" in aggregation_keys
        assert "party" in aggregation_keys
        assert "year" in aggregation_keys
        assert "datatyp" not in aggregation_keys

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

    def test_list_of_facets(self):
        result = self.do_request("/aggs?include_facets=party,year,type")
        facets = list(result["aggregations"].keys())

        assert len(facets) == 4
        assert "party" in facets
        assert "year" in facets
        assert "type" in facets
        assert "corpus_id" in facets

    def test_unused_facets(self):
        result = self.do_request("/aggs?include_facets=party,year,type")
        unused_facets = result["unused_facets"]
        assert "subtitel" in unused_facets
        assert "talare" in unused_facets
        assert "datatyp" in unused_facets
        assert len(unused_facets) == 7

    def test_corpus_id_filter_without_brackets(self):
        result = self.do_request("/aggs?text_filter={\"corpus_id\":\"rd-sou\"}")
        assert len(result["aggregations"].values()) == 4

    def test_text_query(self):
        # this matches only one document in rd-kammakt
        result = self.do_request("/aggs?text_query=skaldjur")
        assert len(result["aggregations"].values()) == 4
        kammakt_found = False
        vivill_found = False
        for corp_bucket in result["aggregations"]["corpus_id"]["buckets"]:
            if corp_bucket["key"] == "vivill":
                vivill_found = True

            if corp_bucket["key"] == "rd-kammakt":
                kammakt_found = True
                count = 1
            else:
                count = 0

            assert corp_bucket["doc_count"] == count

        assert kammakt_found
        # since we did not send exclude_empty_buckets, all corpora should be included
        # TODO this does not work yet
        # assert vivill_found
