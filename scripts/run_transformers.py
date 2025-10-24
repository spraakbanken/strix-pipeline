import os

from snakemake.script import snakemake

from strixpipeline.config import StrixConfig

"""
Run the document vector generation, either local or remote (vector_generation_type)
If remote, "transformers_server" and "transformer_server_dir" must be set
# if local, calls "./run_transformers.sh" in current directory
# if remote calls "./run_transformers.sh" in transformers_dir on transformers_server
# it is up to the user to create and maintain run_transformers.sh
"""

# local or remote
vector_generation_type = snakemake.config.get("vector_generation_type")
corpus = snakemake.config.get("corpus")
local_transformers_dir = snakemake.params.transformers_dir

strix_config = StrixConfig()

if vector_generation_type == "remote":
    if not strix_config.has_attr("transformers_server"):
        raise RuntimeError("Add transformers_server and transformers_server_dir to run remote")
    server = strix_config.transformers_server
    remote_transformers_dir = f"{server}:{strix_config.transformers_server_dir}"
    # move files to server
    os.system(f"scp -r {local_transformers_dir} {remote_transformers_dir}")
    # run document vector generation
    os.system(f"ssh {server} ./run_transformers.sh {corpus}")
    # move files back to source
    os.system(f"scp -r {os.path.join(remote_transformers_dir, corpus, 'vectors')} {local_transformers_dir}")
else:
    os.system(f"./run_transformers.sh {corpus}")
