import json
import sys
import time
from concurrent import futures
import multiprocessing

import elasticsearch
import elasticsearch.helpers
import elasticsearch.exceptions
from elasticsearch import serializer, exceptions
from pathlib import Path

from strixpipeline import xmlparser
from strixpipeline.config import config
import strixpipeline.insertdata as insert_data_strix
import strixpipeline.createindex as create_index_strix
import strixpipeline.runhistory
import logging
import datetime
import os
import orjson


class ORJSONSerializer(serializer.JSONSerializer):
    """Custom serializer using orjson."""

    def dumps(self, data):
        """Serialize data using orjson."""
        if not isinstance(data, (dict, list)):
            raise exceptions.SerializationError(f"Cannot serialize {type(data)}. Must be dict or list.")
        try:
            return orjson.dumps(data).decode("utf-8")
        except Exception as e:
            raise exceptions.SerializationError(f"Orjson serialization error: {e}")

    def loads(self, s):
        """Deserialize data using orjson."""
        try:
            return orjson.loads(s)
        except Exception as e:
            raise exceptions.SerializationError(f"Orjson deserialization error: {e}")


es = elasticsearch.Elasticsearch(
    config.elastic_hosts, timeout=500, retry_on_timeout=True, serializer=ORJSONSerializer()
)


_logger = logging.getLogger(__name__)


def partition_tasks(task_queue, num_tasks):
    threshold = 10000  # KB
    current_size = 0
    current_tasks = []
    work_size_accu = 0

    while True:
        if num_tasks < 1:
            _logger.info("Processing complete")
            break

        (task_data, process_time, work_size) = task_queue.get()
        work_size_accu += work_size
        num_tasks -= 1

        for task in task_data:
            if "_source" in task and "text" in task["_source"]:
                task_size = len(task["_source"]["text"]) / 1024.0  # B to KB
            else:
                task_size = 0.5

            if current_size + task_size >= threshold:
                yield (current_tasks, current_size, work_size_accu)
                current_size = 0
                current_tasks = []

            current_tasks.append(task)
            current_size += task_size
    if current_tasks:
        yield (current_tasks, current_size, work_size_accu)


def process_task(insert_data, size, process_args):
    _task_id = process_args[1]

    try:
        (tasks, delta_t) = insert_data.process(*process_args)
    except Exception:
        _logger.exception("Failed to process %s" % _task_id)
        sys.exit()

    try:
        count = 0
        res = elasticsearch.helpers.streaming_bulk(es, tasks)
        for _ in res:
            count += 1
        _logger.info(f"Added {count} documents to index")
    except Exception as e:
        _logger.exception(e)
        sys.exit()

    _logger.info("Processed id: %s, took %0.1fs" % (_task_id, delta_t))


def process_corpus(index):
    t = time.time()
    insert_data = insert_data_strix.InsertData(index)
    task_data, tot_size = insert_data.prepare_urls()

    with futures.ProcessPoolExecutor(max_workers=min(multiprocessing.cpu_count(), 16)) as executor:
        assert len(task_data)
        _logger.info("Scheduling %s tasks..." % len(task_data))
        for task_type, task_id, size, task in task_data:
            task_args = (task_type, task_id, task)
            executor.submit(process_task, insert_data, size, task_args)

    _logger.info(index + " pipeline complete, took %i min and %i sec. " % divmod(time.time() - t, 60))


def do_run(index):
    strixpipeline.runhistory.create()
    before_t = time.time()

    if not config.corpusconf.is_corpus(index):
        _logger.error('"' + index + " is not a configured corpus.")
        return

    ci = create_index_strix.CreateIndex(index)
    ci.enable_insert_settings()
    process_corpus(index)
    ci.enable_postinsert_settings()

    total_t = time.time() - before_t
    strixpipeline.runhistory.put(
        {
            "index": index,
            "total_time": total_t,
            "elastic_hosts": config.elastic_hosts,
            "timestamp": datetime.datetime.now(),
        }
    )


def check_vector_settings(corpus):
    """
    check that user has set a directory for the transformers data and create directory structure
    """
    if not config.has_attr("transformers_postprocess_dir"):
        _logger.error("transformers_postprocess_dir not set in config")
    Path(os.path.join(config.transformers_postprocess_dir, corpus, "texts")).mkdir(parents=True, exist_ok=True)


def check_vectors_exist(corpus):
    """
    check that a non-empty vectors directory exists inside transformers_postprocess_dir
    """
    check_vector_settings(corpus)
    path = Path(os.path.join(config.transformers_postprocess_dir, corpus, "vectors"))
    return path.is_dir() and any(f.is_file() for f in path.iterdir())


def do_vector_generation(corpus, vector_generation_type):
    """
    First parse the XML:s and extract the text, save it into files
    Then run the document vector generation, either local or remote (vector_generation_type)
    If "transformers_postprocess_server" is set, pipeline will move files from previous run of <corpus>
    to "transformers_postprocess_server_dir" on the given server and run a script on the server
    If "transformers_postprocess_server" is *not* set, it will simply call "./run_transformers.sh" and it is up
    to the user to create and maintain this file
    """
    check_vector_settings(corpus)

    insert_data = insert_data_strix.InsertData(corpus)
    task_data, tot_size = insert_data.prepare_urls()

    corpus_conf = config.corpusconf.get_corpus_conf(corpus)
    split_document = corpus_conf.get("split", "text")
    text_tags = corpus_conf.get("text_tags")

    text_attributes = {"_id": {}}

    for task_type, task_id, size, task in task_data:
        transformer_input = []
        file_path = task["text"]
        for text in xmlparser.parse_pipeline_xml(
            file_path, split_document, {}, text_attributes=text_attributes, text_tags=text_tags
        ):
            doc_id = text["text_attributes"]["_id"]
            transformer_input.append([doc_id, " ".join(text["dump"]).replace("\n", "")])
        with open(os.path.join(config.transformers_postprocess_dir, corpus, f"texts/{task_id}.jsonl"), "w") as fp:
            for text in transformer_input:
                fp.write(f"{json.dumps(text, ensure_ascii=False)}\n")

    text_dir = os.path.join(config.transformers_postprocess_dir, f"{corpus}")
    if vector_generation_type == "remote":
        if not config.has_attr("transformers_postprocess_server"):
            raise RuntimeError(
                "Add transformers_postprocess_server and transformers_postprocess_server_dir to run on remote"
            )
        server = config.transformers_postprocess_server
        vector_server_data_dir = f"{server}:{config.transformers_postprocess_server_dir}"
        # move files to server
        os.system(f"scp -r {text_dir} {vector_server_data_dir}")
        # run document vector generation
        os.system(f"ssh {server} ./run_transformers.sh {corpus}")
        # move files back to source
        os.system(f"scp -r {os.path.join(vector_server_data_dir, corpus, 'vectors')} {text_dir}")
    else:
        os.system(f"./run_transformers.sh {corpus} {text_dir}")


def merge_indices(index):
    _logger.info("Merging segments")
    es.indices.forcemerge(index=index + "," + index + "_terms", max_num_segments=1, request_timeout=10000)
    _logger.info("Done merging segments")


def _get_indices_from_alias(alias_name):
    aliases = es.cat.aliases(name=[alias_name + "*"], format="json")

    alias_exist = False
    index_names = []
    for alias in aliases:
        if alias["alias"] in [alias_name, f"{alias_name}_terms"]:
            alias_exist = True
            index_names.append(alias["index"])

    if not alias_exist:
        _logger.info(f'Alias "{alias_name}", does not exist')
    return index_names


def do_delete(corpus):
    # We expect that an alias only points to *one* index, but if it points to multiple, just remove all of them
    main_indices = _get_indices_from_alias(corpus)
    for index in main_indices:
        _logger.info(f"Deleting index: {index}")
        es.indices.delete(index=index)
        _logger.info("Done deleting index")

    remove_config_file(corpus)


def remove_config_file(corpus):
    settings_dir = config.settings_dir
    fname = os.path.join(settings_dir, f"corpora/{corpus}.yaml")
    if os.path.isfile(fname):
        _logger.info(f"Deleting configuration file: {fname}")
        os.remove(fname)
    else:
        _logger.info(f"Corpus file: '{fname}' does not exist")
