import argparse

from snakemake.api import ConfigSettings, DAGSettings, StorageSettings


def invoke_snakemake(*, target, params, notemp=False):
    from pathlib import Path

    from snakemake.api import (
        ExecutionSettings,
        ResourceSettings,
        SnakemakeApi,
    )
    from snakemake.utils import available_cpu_count

    with SnakemakeApi() as snakemake_api:
        api = snakemake_api.workflow(
            resource_settings=ResourceSettings(cores=available_cpu_count()),
            workdir=Path("."),
            snakefile=Path("strixpipeline/Snakefile"),
            config_settings=ConfigSettings(config=params),
            # mark all output files as temporary
            storage_settings=StorageSettings(notemp=notemp, all_temp=True),
        )
        dag_api = api.dag(DAGSettings(targets=[target], force_incomplete=True))
        dag_api.execute_workflow(
            executor="local",
            execution_settings=ExecutionSettings(),
        )

        # if there where no errors, remove the done-files
        if not notemp:
            output_dir = Path("output") / params["corpus"]
            for f in output_dir.glob("*.done"):
                f.unlink()
            output_dir.rmdir()


def cli():
    def do_add(args):
        params = vars(args)
        del params["func"]
        invoke_snakemake(target="all", params=params, notemp=args.notemp)

    def do_generate_vector_data(args):
        params = vars(args)
        del params["func"]
        invoke_snakemake(target="run_transformers", params=params)

    def do_delete(args):
        import strixpipeline.delete as delete

        corpus = args.corpus
        delete.do_delete(corpus)

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
    add_parser.add_argument(
        "--notemp",
        action="store_true",
        help="Keep temporary Snakemake files after corpus is added. Default: they are removed.",
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
