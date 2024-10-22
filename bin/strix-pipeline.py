import logging
import strixpipeline.loghelper
import strixpipeline.pipeline as pipeline
import strixpipeline.createindex as createindex
import strixpipeline.sparv_decoder as sparv_decoder
from strixpipeline.config import config

if __name__ == "__main__":
    import argparse

    logger = logging.getLogger("strix_pipeline")

    def do_add(args):
        corpus = args.corpus

        # add config file
        sparv_decoder.main(corpus)

        # reload corpus conf
        config.create_corpus_config()

        # add document vectors
        vector_generation_type = args.vector_generation_type
        if vector_generation_type != "none":
            pipeline.do_vector_generation(corpus, vector_generation_type)
        else:
            if not pipeline.check_vectors_exist(corpus):
                raise RuntimeError("Must generate vectors first or use --vector-generation-type local/remote")

        # create new index
        strixpipeline.loghelper.setup_pipeline_logging(f"{corpus}-reindex")
        createindex.create_index(corpus, delete_previous=args.delete_previous_version)

        # run corpus
        strixpipeline.loghelper.setup_pipeline_logging(corpus + "-run")
        pipeline.do_run(corpus)

        pipeline.merge_indices(corpus)

    def do_generate_vector_data(args):
        corpus = args.corpus
        pipeline.do_vector_generation(corpus, args.vector_generation_type)

    def do_delete(args):
        corpus = args.corpus
        pipeline.do_delete(corpus)

    parser = argparse.ArgumentParser(description="Run the pipeline.")
    subparsers = parser.add_subparsers()

    add_parser = subparsers.add_parser("add", help="Add a corpus to Strix.")
    add_parser.add_argument("corpus", help="Corpus to add")
    add_parser.add_argument(
        "--delete-previous-version",
        action="store_true",
        help="Set if you want a previous version of corpus to be deleted (if it exists). Alias for corpus is always deleted.",
    )
    add_parser.add_argument(
        "--vector-generation-type",
        choices=["remote", "local", "none"],
        default="none",
        help="Document vectors can be generated on config.vector_server, locally or not at all.",
    )
    add_parser.set_defaults(func=do_add)

    delete_parser = subparsers.add_parser(
        "delete",
        help="Delete corpus from instance. This will remove both Elasticsearch indices and the configuration files from <settings_dir>/corpora/",
    )
    delete_parser.add_argument("corpus", help="Corpus to delete")
    delete_parser.set_defaults(func=do_delete)

    generate_vector_parser = subparsers.add_parser(
        "generate-vector-data",
        help="Either runs vector data generation locally or offloads vector creation to config.transformers_postprocess_server",
    )
    generate_vector_parser.add_argument("corpus", help="Corpus to update")
    # Same as for add_parser
    generate_vector_parser.add_argument(
        "--vector-generation-type",
        choices=["remote", "local"],
        default="local",
        help="Document vectors can be generated on config.vector_server or locally",
    )
    generate_vector_parser.set_defaults(func=do_generate_vector_data)

    args = parser.parse_args()

    if not hasattr(args, "func"):
        parser.print_help()
    else:
        args.func(args)
