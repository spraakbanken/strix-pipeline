import unittest

from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.connections import connections


class IdTest(unittest.TestCase):

    def test_unique_ids(self):
        """
        all ids should be unique within corpus_id
        """
        es = connections.create_connection()
        for corpus in ["fragelistor", "rd-eun", "rd-flista", "rd-kammakt", "vivill", "wikipedia", "rd-sou", "attasidor"]:
            s = Search(index=corpus, doc_type="text", using=es)
            s = s[0:9999]
            res = s.execute()
            doc_ids = [hit["doc_id"] for hit in res]
            doc_ids.sort()
            latest = None
            for doc_id in doc_ids:
                # print(doc_id)
                if doc_id == latest:
                    assert False, "IDs are not unique in corpus \"" + corpus + "\""
                else:
                    latest = doc_id