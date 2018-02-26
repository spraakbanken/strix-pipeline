# -*- coding: utf-8 -*-
import time
from concurrent import futures
import multiprocessing

import elasticsearch
import elasticsearch.helpers
import elasticsearch.exceptions
from strixpipeline.config import config
import strixpipeline.insertdata as insert_data_strix
import strixpipeline.createindex as create_index_strix
import strixpipeline.runhistory
import logging
import queue
import datetime

QUEUE_SIZE = config.concurrency_queue_size
MAX_UPLOAD_WORKERS = config.concurrency_upload_threads
GROUP_SIZE = config.concurrency_group_size
MAX_GROUP_SIZE_KB = 250 * 1024

elastic_hosts = [config.elastic_hosts]
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


def process(task_queue, insert_data, task_data, corpus_data, limit_to=None):
    executor = futures.ProcessPoolExecutor(max_workers=min(multiprocessing.cpu_count(), 16))

    if limit_to:
        task_data = task_data[:limit_to]
    assert len(task_data)
    _logger.info("Scheduling %s tasks..." % len(task_data))
    for (task_type, task_id, size, task) in task_data:
        task_args = (task_type, task_id, task, corpus_data)
        executor.submit(process_task, insert_data, task_queue, size, task_args)


def get_content_of_bulk(task_chunk):
    docs = {}
    files = set()
    for task in task_chunk:
        if task["_index"].endswith("_terms"):
            doc_type = task["doc_type"]
            doc_id = task["doc_id"]
        else:
            doc_type = task["_type"]
            if doc_type == "text":
                files.add(task["_source"]["original_file"])
                doc_id = task["_source"]["doc_id"]
            else:
                doc_id = task["_id"]

        if doc_type not in docs:
            docs[doc_type] = set()

        docs[doc_type].add(doc_id)

    return docs, files


def upload_executor(task_queue, tot_size, num_tasks):

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

            for (task_chunk, size) in chunks:
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
                        (docs, files) = error_obj
                        if files:
                            _logger.error("The following files need to be rerun:")
                            for file_name in files:
                                _logger.error(file_name)
                        if docs:
                            _logger.error("The following documents need to be deleted and added again (terms and document):")
                            for doc_type, doc_ids in docs.items():
                                for doc_id in doc_ids:
                                    type_output = doc_type + "-" if doc_type != "text" else ""
                                    _logger.error(type_output + doc_id)
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
        elasticsearch.helpers.bulk(es, tasks)
    except Exception as e:
        _logger.exception("Error in bulk upload")
        error_obj = get_content_of_bulk(tasks)

    return len(tasks), time.time() - insert_t, error_obj


def process_corpus(index, limit_to=None, doc_ids=()):
    t = time.time()
    insert_data = insert_data_strix.InsertData(index)

    task_data, tot_size = insert_data.prepare_urls(doc_ids)

    from multiprocessing import Manager
    with Manager() as manager:
        task_queue = manager.Queue(maxsize=QUEUE_SIZE)
        process(task_queue, insert_data, task_data, {}, limit_to)
        upload_executor(task_queue, tot_size, len(task_data))

    _logger.info(index + " pipeline complete, took %i min and %i sec. " % divmod(time.time() - t, 60))


def reindex_corpus(source_alias, target_index):
    body = {
        "source": {
            "index": source_alias
        },
        "dest": {
            "index": target_index,
            "version_type": "internal"
        }
    }
    task = es.reindex(body=body, requests_per_second=20000, wait_for_completion=False)
    task_id = task["task"]
    completed = False
    while not completed:
        time.sleep(10)
        task_info = es.tasks.get(task_id)
        _logger.info("Waiting on reindexing")
        completed = task_info["completed"]
    _logger.info("Reindexing done")


def delete_index(alias):
    es.indices.delete(index=alias, ignore=[400, 404])


def setup_alias(alias_name, index_name):
    es.indices.put_alias(index=index_name, name=alias_name)


def delete_index_by_prefix(prefix):
    es.indices.delete(prefix + "_*")


def do_run(index, doc_ids=(), limit_to=None):
    strixpipeline.runhistory.create()
    before_t = time.time()

    if not config.corpusconf.is_corpus(index):
        _logger.error("\"" + index + " is not a configured corpus.")
        return

    ci = create_index_strix.CreateIndex(index)
    ci.enable_insert_settings()
    process_corpus(index, limit_to=limit_to, doc_ids=doc_ids)
    ci.enable_postinsert_settings()

    total_t = time.time() - before_t
    strixpipeline.runhistory.put({
        "index": index,
        "total_time": total_t,
        "doc_ids": doc_ids,
        "limit_to": limit_to,
        "group_size": GROUP_SIZE,
        "queue_size": QUEUE_SIZE,
        "upload_threads": MAX_UPLOAD_WORKERS,
        "max_group_size": MAX_GROUP_SIZE_KB,
        "elastic_hosts": config.elastic_hosts,
        "timestamp": datetime.datetime.now()
    })


def recreate_indices(indices):
    for index in indices:
        if config.corpusconf.is_corpus(index):
            delete_index_by_prefix(index)
            ci = create_index_strix.CreateIndex(index)
            try:
                index_name = ci.create_index()
                setup_alias(index, index_name)
            except elasticsearch.exceptions.TransportError as e:
                _logger.exception("transport error")
                raise e
        else:
            _logger.error("\"" + index + "\" is not a configured corpus")


def reindex(indices):
    for alias in indices:
        ci = create_index_strix.CreateIndex(alias, reindexing=True)
        new_index_name = ci.create_index()
        ci.enable_insert_settings(index_name=new_index_name)
        reindex_corpus(alias, new_index_name)
        ci.enable_postinsert_settings(index_name=new_index_name)
        delete_index(alias)
        setup_alias(alias, new_index_name)


def remove_by_filename(index, filenames):
    """
    Assumes texts have type "text" and fields "original_file" and "doc_id"
    """
    query = {
        "query": {
            "terms": {
                "original_file": filenames
            }
        }
    }

    delete_texts_ids = set()
    for doc in elasticsearch.helpers.scan(es, index=index, doc_type="text", query=query, _source_include=["doc_id"]):
        doc_id = doc["_source"]["doc_id"]
        delete_texts_ids.add(doc_id)
    remove_by_doc_id(index, list(delete_texts_ids))


def remove_by_doc_id(index, doc_ids):
    """
    Assumes texts have typ "text" and field "doc_id"
    """
    if not doc_ids:
        _logger.info("Nothing to delete")
        return

    query = {
        "query": {
            "terms": {
                "doc_id": doc_ids
            }
        }
    }

    _logger.info("Deleting doc_ids: " + ",".join(doc_ids))
    es.delete_by_query(index=index, doc_type="text", body=query, conflicts="proceed")
    es.delete_by_query(index=index + "_terms", doc_type="term", body=query, conflicts="proceed")
    es.indices.forcemerge(index=index + "," + index + "_terms")


def merge_indices(index):
    _logger.info("Merging segments")
    es.indices.forcemerge(index=index + "," + index + "_terms", max_num_segments=1, request_timeout=10000)
    _logger.info("Done merging segments")
