# -*- coding: utf-8 -*-
import os
import logging
import strix.loghelper
import strix.pipeline.pipeline as pipeline

os.environ["PYTHONIOENCODING"] = "utf_8"


if __name__ == '__main__':
    import argparse
    logger = logging.getLogger("strix_pipeline")

    def do_run(args):
        doc_ids = args.doc_ids if args.doc_ids else []
        index = args.index
        limit_to = args.limit_to
        strix.loghelper.setup_pipeline_logging(index + "-run")
        pipeline.do_run(index, doc_ids, limit_to)

    def do_recreate(args):
        indices = args.index
        if indices:
            strix.loghelper.setup_pipeline_logging("|".join(indices) + "-reindex")
            pipeline.recreate_indices(indices)

    def do_delete(args):
        index = args.index
        filenames = args.filenames if args.filenames else []
        doc_ids = args.doc_ids if args.doc_ids else []
        pipeline.remove_by_filename(index, filenames)
        pipeline.remove_by_doc_id(index, doc_ids)

    def do_merge(args):
        index = args.index
        pipeline.merge_indices(index)

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

    delete_parser = subparsers.add_parser("delete", help="Delete documents by document id or filename")
    delete_parser.add_argument("--index", required=True, help="Index to delete documents in")
    delete_parser.add_argument("--filenames", nargs="*", help="Original file names to delete by")
    delete_parser.add_argument('--doc-ids', nargs='*', help="Document ids to delete by")
    delete_parser.set_defaults(func=do_delete)

    merge_parser = subparsers.add_parser("merge", help="Run forcemerge on index")
    merge_parser.add_argument("--index", required=True, help="Index to merge")
    merge_parser.set_defaults(func=do_merge)

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
    else:
        args.func(args)
