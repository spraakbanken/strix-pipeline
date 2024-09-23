# -*- coding: utf-8 -*-
import os
import logging
import strixpipeline.loghelper
import strixpipeline.pipeline as pipeline
import strixpipeline.createindex as createindex

os.environ["PYTHONIOENCODING"] = "utf_8"


if __name__ == "__main__":
    import argparse

    logger = logging.getLogger("strix_pipeline")

    def do_run(args):
        doc_ids = args.doc_ids if args.doc_ids else []
        index = args.index
        limit_to = args.limit_to if hasattr(args, "limit_to") else None
        strixpipeline.loghelper.setup_pipeline_logging(index + "-run")
        pipeline.do_run(index, doc_ids, limit_to)

    def do_recreate(args):
        indices = args.index
        if indices:
            strixpipeline.loghelper.setup_pipeline_logging(
                "|".join(indices) + "-reindex"
            )
            createindex.recreate_indices(indices)

    def do_merge(args):
        index = args.index
        pipeline.merge_indices(index)

    def do_delete(args):
        corpus = args.corpus
        pipeline.do_delete(corpus)

    def do_all(args):
        do_recreate(args)
        do_run(args)
        do_merge(args)

    # Parse command line arguments

    parser = argparse.ArgumentParser(description="Run the pipeline.")
    subparsers = parser.add_subparsers()

    # *** Run parser ***
    run_parser = subparsers.add_parser("run", help="Run the pipeline with input files.")
    run_parser.add_argument("--index", required=True, help="Index to run files on")

    run_parser.add_argument(
        "--limit-to", type=int, help="only process so many of the input urls."
    )

    run_parser.add_argument(
        "--doc-ids",
        nargs="*",
        help="An optional list of IDs (filenames). Default is to run all files. ",
    )

    run_parser.set_defaults(func=do_run)

    # *** Reset parser ***
    reset_parser = subparsers.add_parser(
        "recreate", help="delete all data in given corpora and create index"
    )

    reset_parser.add_argument(
        "--index",
        nargs="+",
        help="Deletes index and everything in it, then recreates it.",
    )

    reset_parser.set_defaults(func=do_recreate)

    merge_parser = subparsers.add_parser("merge", help="Run forcemerge on index")
    merge_parser.add_argument("--index", required=True, help="Index to merge")
    merge_parser.set_defaults(func=do_merge)

    all_parser = subparsers.add_parser(
        "all", help="Run recreate, run and merge on index"
    )
    all_parser.add_argument("--index", required=True, help="Index to create")
    all_parser.add_argument(
        "--doc-ids",
        nargs="*",
        help="An optional list of IDs (filenames). Default is to run all files. ",
    )
    all_parser.set_defaults(func=do_all)

    delete_parser = subparsers.add_parser(
        "delete",
        help="Delete corpus from instance. This will remove both Elasticsearch indices and the configuration files from <settings_dir>/corpora/",
    )
    delete_parser.add_argument("corpus", help="Corpus to delete")
    delete_parser.set_defaults(func=do_delete)

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
    else:
        args.func(args)
