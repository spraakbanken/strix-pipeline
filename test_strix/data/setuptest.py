import logging
import strix.pipeline.pipeline as pipeline

corpora = ["fragelistor", "rd-eun", "rd-flista", "rd-kammakt", "vivill", "wikipedia"]


_logger = logging.getLogger(__name__)


def insert_test_data_from_xml():
    pipeline.recreate_indices(corpora)
    for corpus in corpora:
        pipeline.do_run(corpus)


if __name__ == "__main__":
    insert_test_data_from_xml()
