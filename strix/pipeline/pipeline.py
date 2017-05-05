# -*- coding: utf-8 -*-
import time
from concurrent import futures
import multiprocessing

import elasticsearch
import elasticsearch.helpers
import elasticsearch.exceptions
from strix.config import config
import strix.pipeline.insertdata as insert_data_strix
import logging
import queue

QUEUE_SIZE = config.concurrency_queue_size
MAX_UPLOAD_WORKERS = config.concurrency_upload_threads
GROUP_SIZE = config.concurrency_group_size
MAX_GROUP_SIZE_KB = 250 * 1024

elastic_hosts = [config.elastic_hosts]
es = elasticsearch.Elasticsearch(config.elastic_hosts, timeout=500)

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


def format_content_of_bulk(task_chunk):
    terms = {}
    docs = {}
    for chunk in task_chunk:
        if chunk["_index"].endswith("_terms"):
            doc_type = chunk["doc_type"]
            doc_id = chunk["doc_id"]
            if doc_type not in terms:
                terms[doc_type] = []
            if doc_id not in terms[doc_type]:
                terms[doc_type].append(doc_id)
        else:
            doc_type = chunk["_type"]
            doc_id = chunk["_id"]
            if doc_type not in docs:
                docs[doc_type] = []
            if doc_id not in docs[doc_type]:
                docs[doc_type].append(doc_id)
    bulk_summary = "\nFrom the \"_terms\"-index:\n"
    for doc_type, doc_ids in terms.items():
        bulk_summary += "- From doc type \"" + doc_type + "\": " + ", ".join(doc_ids) + "\n"
    bulk_summary += "\nFailed documents:\n"
    for doc_type, doc_ids in docs.items():
        bulk_summary += "- From doc type \"" + doc_type + "\": " + ", ".join(doc_ids) + "\n"
    bulk_summary += "\n"
    return bulk_summary


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
                # TODO: use task_chunk for logging extra exception data.
                size_accu = future_map.pop(future)
                if future.exception() is None:
                    chunk_len, t = future.result()
                    _logger.info("Bulk uploaded a chunk of length %s, took %0.1fs" % (chunk_len, t))

                    if tot_size > 0:
                        _logger.info("%.1f%%" % (100 * (size_accu / tot_size)))
                        _logger.info("------------------")


def bulk_insert(tasks):
    insert_t = time.time()
    try:
        elasticsearch.helpers.bulk(es, tasks)
        return len(tasks), time.time() - insert_t
    except:
        _logger.exception("Bulk upload error. Bulk contained:\n%s" % format_content_of_bulk(tasks))
        raise


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
    es.indices.delete(prefix + "*")
