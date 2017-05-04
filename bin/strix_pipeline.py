# -*- coding: utf-8 -*-
import os
import logging
import elasticsearch
import strix.loghelper
import strix.pipeline.pipeline as pipeline
import strix.pipeline.createindex as createindex

os.environ["PYTHONIOENCODING"] = "utf_8"


if __name__ == '__main__':
    import argparse
    logger = logging.getLogger("strix_pipeline")

    def do_run(args):
        doc_ids = args.doc_ids if args.doc_ids else []
        strix.loghelper.setup_pipeline_logging(args.index + "-run")
        ci = createindex.CreateIndex(args.index)
        ci.enable_insert_settings()
        pipeline.process_corpus(args.index, limit_to=args.limit_to, doc_ids=doc_ids)
        ci.enable_postinsert_settings()

    def do_recreate(args):
        if args.index:
            strix.loghelper.setup_pipeline_logging("|".join(args.index) + "-reindex")
            for index in args.index:
                pipeline.delete_index_by_prefix(index)
                ci = createindex.CreateIndex(index)
                try:
                    index_name = ci.create_index()
                    pipeline.setup_alias(index, index_name)
                except elasticsearch.exceptions.TransportError as e:
                    logger.exception("transport error")
                    raise e

    def do_reindex(args):
        if args.index:
            strix.loghelper.setup_pipeline_logging("|".join(args.index) + "-reindex-data")
            for alias in args.index:
                ci = createindex.CreateIndex(alias, reindexing=True)
                new_index_name = ci.create_index()
                ci.enable_insert_settings(index_name=new_index_name)
                pipeline.reindex_corpus(alias, new_index_name)
                ci.enable_postinsert_settings(index_name=new_index_name)
                pipeline.delete_index(alias)
                pipeline.setup_alias(alias, new_index_name)

    # Parse command line arguments

    parser = argparse.ArgumentParser(description='Run the pipeline.')
    subparsers = parser.add_subparsers()

    # *** Run parser ***
    run_parser = subparsers.add_parser("run", help="Run the pipeline with input files.")
    run_parser.add_argument('--index', required=True,
                            help='Index to run files on')

    run_parser.add_argument('--limit-to', type=int,
                            help='only process so many of the input urls.')

    run_parser.add_argument('--doc-ids', nargs='*',
                            help='An optional list of IDs (filenames). Default is to run all files. ')

    run_parser.set_defaults(func=do_run)

    # *** Reset parser ***
    reset_parser = subparsers.add_parser('recreate',
                                         help='delete all data in given corpora and create index')

    reset_parser.add_argument("--index", nargs="+",
                              help="Deletes index and everything in it, then recreates it.")

    reset_parser.set_defaults(func=do_recreate)

    reindex_parser = subparsers.add_parser("reindex", help="Reindex all data from old index into new index")
    reindex_parser.add_argument("--index", nargs="+", help="Copy this index to a new index")
    reindex_parser.set_defaults(func=do_reindex)

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
    else:
        args.func(args)
