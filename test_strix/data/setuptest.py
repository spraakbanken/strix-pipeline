import logging
import strix.pipeline.pipeline as pipeline

#<corpora = ["fragelistor", "rd-eun", "rd-flista", "rd-kammakt", "vivill", "wikipedia", "rd-sou", "attasidor"]
corpora = ["attasidor", "vivill"]

_logger = logging.getLogger(__name__)


def insert_test_data_from_xml():
    for corpus in corpora:
        pipeline.recreate_indices([corpus])
        pipeline.do_run(corpus)
        pipeline.merge_indices(corpus)


if __name__ == "__main__":
    insert_test_data_from_xml()
