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
        decode = rv.data.decode()
        return json.loads(decode)

    def test_strix_highlight_1(self):
        result = self.do_request("/search?text_query=framtid&corpora=vivill&exclude=dump,token_lookup,lines")
        for hit in result["data"]:
            assert "highlight" in hit
            for highlight in hit["highlight"]["highlight"]:
                assert len(highlight["match"]) == 1
                assert highlight["match"][0]["attrs"]["lemma"][0] == "framtid"
        assert result

    def test_strix_highlight_2(self):
        result = self.do_request("/search?text_query=Sveriges framtid&corpora=vivill&exclude=dump,token_lookup,lines")
        for hit in result["data"]:
            assert "highlight" in hit
            for highlight in hit["highlight"]["highlight"]:
                assert len(highlight["match"]) == 2
                assert highlight["match"][0]["attrs"]["lemma"][0] == "Sverige"
                assert highlight["match"][1]["attrs"]["lemma"][0] == "framtid"
        assert result

    # TODO this fails because one highlight is returned that do not contain an <em> tag??
    def test_simple_highlight(self):
        result = self.do_request("/search?text_query=framtid&corpora=vivill&exclude=dump,token_lookup,lines&simple_highlight=true")
        for hit in result["data"]:
            assert "highlight" in hit
            highlights = hit["highlight"]["highlight"]
            assert len(highlights) <= 5
            for highlight in highlights:
                assert "<em>" in highlight
                word = highlight.split("<em>")[1].split("</em>")[0].lower()
                assert word.startswith("framtid")
        hit = result["data"][1]
        assert hit["highlight"]["highlight"][3] == "inte förstörs och investera i <em>framtidens</em> hållbara jobb. Under mandatperioden"

    def test_simple_highlight2(self):
        result = self.do_request("/search?text_query=Därför måste fler&corpora=vivill&exclude=dump,token_lookup,lines&simple_highlight=true")
        hit = result["data"][0]
        assert hit["highlight"]["highlight"][0] == "skapar morgondagens omistliga klassiker. <em>Därför måste många</em> få möjlighet att skapa."
        assert hit["highlight"]["highlight"][1] == "och utan långa väntetider. <em>Därför måste fler</em> åtgärder prövas för att locka"
        hit = result["data"][1]
        assert hit["highlight"]["highlight"][0] == "enskild såväl som gemensam. <em>Därför måste fler</em> få jobb eller skapa sitt"