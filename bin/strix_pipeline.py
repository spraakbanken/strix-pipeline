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
        pipeline.process_corpus(args.index, limit_to=args.limit_to, doc_ids=doc_ids)

    def do_reset(args):
        if args.index:
            strix.loghelper.setup_pipeline_logging("|".join(args.index) + "-reindex")
            for index in args.index:
                ci = createindex.CreateIndex(index)
                try:
                    ci.create_index()
                except elasticsearch.exceptions.TransportError as e:
                    logger.exception("transport error")
                    if e.error == "access_control_exception":
                        import textwrap
                        logger.error(textwrap.dedent("""\
                            Your analyzers/stems.txt file can't be read. 
                            Please add the following to you java.policy file (replace STRIX_PATH
                            with the actual path):

                            grant {
                                permission java.io.FilePermission "/STRIX_PATH/resources/analyzers/stems.txt", "read";
                            };

                            Then restart elasticsearch. 
                            """))
                    raise e

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
    reset_parser = subparsers.add_parser('reindex',
                                         help='reset the index, recreate type mapping or reindex data.')

    reset_parser.add_argument("--index", nargs="+",
                              help="Deletes index and everything in it, then recreates it.")

    reset_parser.set_defaults(func=do_reset)

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
    else:
        args.func(args)
