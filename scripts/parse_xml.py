from pathlib import Path

import orjson
from snakemake.script import snakemake

from strixpipeline.config import StrixConfig
from strixpipeline.document.insertdata import process_work

input_file = Path(snakemake.input[1])
# used for the partial ES-document (document vector missing)
doc_output_file = Path(snakemake.output[0])
# used for input text to transformers
text_output_file = Path(snakemake.output[1])

corpus = snakemake.config.get("corpus")

name = snakemake.wildcards.name

config = StrixConfig()
corpus_conf = config.corpusconf.get_corpus_conf(corpus)

result = process_work(config, corpus_conf, corpus, name, {"text": input_file})

with open(doc_output_file, "wb") as fp:
    fp.write(orjson.dumps(result))

transformer_input = []
for text in result[0]:
    doc_id = text["_source"]["doc_id"]
    transformer_input.append([doc_id, " ".join(text["_source"]["dump"]).replace("\n", "")])
with open(text_output_file, "wb") as fp:
    for row in transformer_input:
        fp.write(orjson.dumps(row))
        fp.write(b"\n")
