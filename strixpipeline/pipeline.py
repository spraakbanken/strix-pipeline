import glob
import json
import time
from concurrent import futures
import multiprocessing

import elasticsearch
import elasticsearch.helpers
import elasticsearch.exceptions
from elasticsearch_dsl import Search, Q
from pathlib import Path
from strixpipeline.config import config
import strixpipeline.insertdata as insert_data_strix
import strixpipeline.createindex as create_index_strix
import strixpipeline.runhistory
import logging
import queue
import datetime
import os

QUEUE_SIZE = config.concurrency_queue_size
MAX_UPLOAD_WORKERS = config.concurrency_upload_threads
GROUP_SIZE = config.concurrency_group_size
MAX_GROUP_SIZE_KB = 250 * 1024

es = elasticsearch.Elasticsearch(config.elastic_hosts, timeout=500, retry_on_timeout=True)

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


def process_task(insert_data, task_queue, size, process_args):
    _task_id = process_args[1]

    try:
        (tasks, delta_t) = insert_data.process(*process_args)
    except Exception:
        _logger.exception("Failed to process %s" % _task_id)
        tasks = []
        delta_t = -1

    try:
        task_queue.put((tasks, delta_t, size), block=True)

        if tasks:
            _logger.info("Processed id: %s, took %0.1fs" % (_task_id, delta_t))
        else:
            _logger.error("Did not process id: %s" % _task_id)
    except queue.Full:
        _logger.exception("queue.put exception")
        raise


def process(task_queue, insert_data, task_data):
    executor = futures.ProcessPoolExecutor(max_workers=min(multiprocessing.cpu_count(), 16))

    assert len(task_data)
    _logger.info("Scheduling %s tasks..." % len(task_data))
    for task_type, task_id, size, task in task_data:
        task_args = (task_type, task_id, task)
        executor.submit(process_task, insert_data, task_queue, size, task_args)


def get_content_of_bulk(task_chunk):
    docs = set()
    for task in task_chunk:
        if task["_index"].endswith("_terms"):
            doc_id = task["doc_id"]
        else:
            if "_source" in task and "doc_id" in task["_source"]:
                doc_id = task["_source"]["doc_id"]
            elif "_id" in task:
                doc_id = task["_id"]
            else:
                doc_id = "unknown"

        docs.add(doc_id)
    return docs


def upload_executor(task_queue, tot_size, num_tasks):
    _logger.info("Starting upload executor process")
    with futures.ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS) as executor:

        def grouper(max_group_size, tasks):
            current_group_length = 0
            current_group_size = 0
            current_group = []
            while True:
                try:
                    (task, task_size, accu_size) = next(tasks)
                    if current_group_size + task_size > MAX_GROUP_SIZE_KB or current_group_length == max_group_size:
                        yield current_group
                        current_group = []
                        current_group_size = 0
                        current_group_length = 0

                    current_group.append((task, accu_size))
                    current_group_length += 1
                    current_group_size += task_size
                except StopIteration:
                    yield current_group
                    break

        chunk_iter = partition_tasks(task_queue, num_tasks)

        grouped_chunks = grouper(GROUP_SIZE, chunk_iter)

        for chunks in grouped_chunks:
            future_map = {}
            chunks = filter(bool, chunks)

            for task_chunk, size in chunks:
                if not task_chunk:
                    continue
                # TODO: this bulk_insert should be replaced with
                # elasticsearch.helpers.streaming_bulk which should allow for getting
                # rid of the rather complex bulk packet size calculations in this method.
                future = executor.submit(bulk_insert, task_chunk)
                future_map[future] = size
            _logger.info("------------------")
            for future in futures.as_completed(future_map):
                size_accu = future_map.pop(future)
                if future.exception() is None:
                    chunk_len, t, error_obj = future.result()
                    if not error_obj:
                        _logger.info("Bulk uploaded a chunk of length %s, took %0.1fs" % (chunk_len, t))
                    else:
                        _logger.error("Failed bulk upload of a chunk.")
                        _logger.error(
                            "The following documents need to be deleted and added again (terms and document):"
                        )
                        docs, exception = error_obj
                        for doc_id in docs:
                            _logger.error(doc_id)
                        _logger.error(exception)
                else:
                    try:
                        raise future.exception() from None
                    except Exception:
                        _logger.exception("Failed bulk upload of a chunk.")
                if tot_size > 0:
                    _logger.info("%.1f%%" % (100 * (size_accu / tot_size)))
                    _logger.info("------------------")


def bulk_insert(tasks):
    insert_t = time.time()
    error_obj = None
    try:
        size = len(tasks)
        _logger.info(f"Doing bulk insert on {size} documents")
        elasticsearch.helpers.bulk(es, tasks)
    except Exception as e:
        _logger.exception("Error in bulk upload")
        error_obj = get_content_of_bulk(tasks), e
    return len(tasks), time.time() - insert_t, error_obj


def process_corpus(index):
    t = time.time()
    insert_data = insert_data_strix.InsertData(index)

    task_data, tot_size = insert_data.prepare_urls()

    from multiprocessing import Manager

    with Manager() as manager:
        task_queue = manager.Queue(maxsize=QUEUE_SIZE)
        process(task_queue, insert_data, task_data)
        upload_executor(task_queue, tot_size, len(task_data))

    _logger.info(index + " pipeline complete, took %i min and %i sec. " % divmod(time.time() - t, 60))


def do_run(index):
    strixpipeline.runhistory.create()
    before_t = time.time()

    if not config.corpusconf.is_corpus(index):
        _logger.error('"' + index + " is not a configured corpus.")
        return

    # check that user has set a directory for the transformers data and create directory structure
    if not config.has_attr("transformers_postprocess_dir"):
        _logger.error("transformers_postprocess_dir not set in config")
    Path(os.path.join(config.transformers_postprocess_dir, index, "texts")).mkdir(parents=True, exist_ok=True)

    ci = create_index_strix.CreateIndex(index)
    ci.enable_insert_settings()
    process_corpus(index)
    ci.enable_postinsert_settings()

    total_t = time.time() - before_t
    strixpipeline.runhistory.put(
        {
            "index": index,
            "total_time": total_t,
            "group_size": GROUP_SIZE,
            "queue_size": QUEUE_SIZE,
            "upload_threads": MAX_UPLOAD_WORKERS,
            "max_group_size": MAX_GROUP_SIZE_KB,
            "elastic_hosts": config.elastic_hosts,
            "timestamp": datetime.datetime.now(),
        }
    )


def do_vector_generation(corpus, vector_generation_type):
    """
    If "transformers_postprocess_server" is set, pipeline will move files from previous run of <corpus>
    to "transformers_postprocess_server_dir" on the given server and run a script on the server
    If "transformers_postprocess_server" is *not* set, it will simply call "./run_transformers.sh" and it is up
    to the user to create and maintain this file
    """
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

    settings_dir = config.settings_dir
    fname = os.path.join(settings_dir, f"corpora/{corpus}.yaml")
    if os.path.isfile(fname):
        _logger.info(f"Deleting configuration file: {fname}")
        os.remove(fname)
    else:
        _logger.info(f"Corpus file: '{fname}' does not exist")


def do_add_vector_data(corpus):
    files = glob.glob(os.path.join(config.transformers_postprocess_dir, f"{corpus}/vectors/*"))
    count = 0
    for file in files:
        with open(file) as fp:
            for line in fp:
                [doc_id, vector] = json.loads(line)
                # TODO slow to first get the document and then update it
                s = Search(index=corpus, using=es)
                s = s.query(Q("term", doc_id=doc_id))
                for hit in s.execute():
                    _logger.info(f"Adding document vectors for {doc_id} ({count})")
                    es_doc_id = hit.meta.id
                    es.update(index=corpus, id=es_doc_id, body={"doc": {"sent_vector": vector}})
                    count += 1
                    break
    merge_indices(corpus)
