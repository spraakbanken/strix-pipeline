# -*- coding: utf-8 -*-
import time
from concurrent import futures
import multiprocessing

import elasticsearch
import elasticsearch.helpers
import elasticsearch.exceptions
from strix.config import config
import itertools
import strix.pipeline.insertdata as insert_data_strix
import logging
import queue

QUEUE_SIZE = config.concurrency_queue_size
MAX_UPLOAD_WORKERS = config.concurrency_upload_threads
GROUP_SIZE = config.concurrency_group_size

elastic_hosts = [config.elastic_hosts]
es = elasticsearch.Elasticsearch(config.elastic_hosts, timeout=120)

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

        (task_type, task_id, task_data, process_time, work_size) = task_queue.get()
        work_size_accu += work_size
        num_tasks -= 1

        for task in task_data:
            if "_source" in task:
                task_size = len(task["_source"]["text"]) / 1024.0  # B to KB
            else:
                task_size = 0.5

            current_tasks.append(task)
            current_size += task_size

            if current_size >= threshold:
                yield (task_type, task_id, current_tasks, work_size_accu)
                current_size = 0
                current_tasks = []
    if current_tasks:
        yield (task_type, task_id, current_tasks, work_size_accu)


def process_task(insert_data, task_queue, size, process_args):
    _task_id = process_args[1]

    try:
        (task_type, task_id, tasks, delta_t) = insert_data.process(*process_args)
    except Exception:
        _logger.exception("Failed to process %s" % _task_id)
        raise

    try:
        task_queue.put((task_type, task_id, tasks, delta_t, size), block=True)
        _logger.info("Processed id: %s, took %0.1fs" % (_task_id, delta_t))
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
    return task_queue


def upload_executor(task_queue, tot_size, num_tasks):

    with futures.ThreadPoolExecutor(max_workers=MAX_UPLOAD_WORKERS) as executor:

        def grouper(n, iterable):
            """grouper(3, "ABCDEFG", "x") --> ABC DEF Gxx"""
            args = [iter(iterable)] * n
            return itertools.zip_longest(*args)

        chunk_iter = partition_tasks(task_queue, num_tasks)

        grouped_chunks = grouper(GROUP_SIZE, chunk_iter)

        for chunks in grouped_chunks:
            future_map = {}
            chunks = filter(bool, chunks)

            for (task_type, task_id, task_chunk, size) in chunks:
                if not task_chunk:
                    continue
                # TODO: this bulk_insert should be replaced with 
                # elasticsearch.helpers.streaming_bulk which should allow for getting
                # rid of the rather complex bulk packet size calculations in this method. 
                future = executor.submit(bulk_insert, task_chunk)
                future_map[future] = (task_type, task_id, task_chunk, size)
            for future in futures.as_completed(future_map):
                # TODO: use task_chunk for logging extra exception data.
                task_type, task_id, task_chunk, size_accu = future_map.pop(future)
                if future.exception() is not None:
                    try:
                        raise future.exception()
                    except:
                        _logger.exception("Bulk upload error: %s:%s" % (task_type, task_id))
                else:
                    chunk, t = future.result()
                    _logger.info("Bulk uploaded a chunk of length %s, took %0.1fs" % (len(chunk), t))

                    if tot_size > 0:
                        _logger.info("%.1f%%" % (100 * (size_accu / tot_size)))
                        _logger.info("------------------")


def bulk_insert(tasks):
    insert_t = time.time()
    elasticsearch.helpers.bulk(es, tasks)
    return tasks, time.time() - insert_t


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
