# -*- coding: utf-8 -*-
import logging
import os
import sys
import time
from concurrent import futures
import multiprocessing

import elasticsearch
from strix.config import config
import itertools
import strix.pipeline.insertdata as insert_data_strix


elastic_hosts = [config.elastic_hosts]
es = elasticsearch.Elasticsearch(config.elastic_hosts, timeout=120)


class MsgCounterHandler(logging.Handler):
    levelcount = None

    def __init__(self, *args, **kwargs):
        super(MsgCounterHandler, self).__init__(*args, **kwargs)
        self.levelcount = {}

    def emit(self, record):
        l = record.levelname
        self.levelcount.setdefault(l, 0)
        self.levelcount[l] += 1


def setup_logger():
    logger = logging.getLogger("strix.pipeline")
    # Show all message levels from 'debug' to 'critical'
    logger.setLevel(logging.DEBUG)

    os.makedirs("logs", exist_ok=True)

    # Set mode to 'a' to log
    fh = logging.FileHandler("logs/pipeline.log", mode="w", encoding="UTF-8")
    fh.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)

    errh = logging.FileHandler("logs/pipeline.err.log", mode="w", encoding="UTF-8")
    errh.setLevel(logging.ERROR)

    # console logger
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(logging.NOTSET)

    logger.addHandler(ch)
    logger.addHandler(fh)
    logger.addHandler(errh)
    logger.addHandler(logcounter)

    return logger

logcounter = MsgCounterHandler()
logger = setup_logger()


def partition_tasks(queue, num_tasks):

    THRESHOLD = 10000  # KB
    current_size = 0
    current_tasks = []
    work_size_accu = 0

    # for task in task_data:
    while True:
        if num_tasks < 1:
            logger.info("Processing complete")
            break
        try:
            print("queue.get size", queue.qsize())
            (task_type, task_id, task_data, process_time, work_size) = queue.get()
            work_size_accu += work_size
            num_tasks -= 1
        except Exception as e:  # queue.Empty
            logger.exception("queue.get exception")
            break

        for task in task_data:
            try:
                task_size = len(task["_source"]["text"]) / 1024.0  # B to KB
            except:
                task_size = 0.5

            current_tasks.append(task)
            current_size += task_size

            if current_size >= THRESHOLD:
                yield (task_type, task_id, current_tasks, work_size_accu)
                current_size = 0
                current_tasks = []
    if current_tasks:
        yield (task_type, task_id, current_tasks, work_size_accu)


def process_task(insert_data, queue, size, process_args):
    _task_id = process_args[1]

    try:
        (task_type, task_id, tasks, delta_t) = insert_data.process(*process_args)
    except Exception as e:
        logger.exception("Failed to process %s" % _task_id)
        raise

    try:
        queue.put((task_type, task_id, tasks, delta_t, size), block=True)
        logger.info("Processed id: %s, took %0.1fs" % (_task_id, delta_t))
    except Exception:  # queue.Full
        logger.exception("queue.put exception")
        raise


def process(queue, insert_data, task_data, corpus_data, tot_size, limit_to=None):
    executor = futures.ProcessPoolExecutor(max_workers=min(multiprocessing.cpu_count(), 16))

    if limit_to:
        task_data = task_data[:limit_to]
    assert len(task_data)
    logger.info("Scheduling %s tasks..." % len(task_data))
    for (task_type, task_id, size, task) in task_data:
        task_args = (task_type, task_id, task, corpus_data)
        executor.submit(process_task, insert_data, queue, size, task_args)
    return queue


def upload_executor(insert_data, queue, tot_size, num_tasks):
    tot_uploaded = 0

    with futures.ThreadPoolExecutor(max_workers=config.concurrency_upload_threads) as executor:

        def grouper(n, iterable, fillvalue=None):
            """grouper(3, "ABCDEFG", "x") --> ABC DEF Gxx"""
            args = [iter(iterable)] * n
            return itertools.zip_longest(fillvalue=fillvalue, *args)

        chunk_iter = partition_tasks(queue, num_tasks)

        grouped_chunks = grouper(config.concurrency_group_size, chunk_iter)

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
                        logger.exception("Bulk upload error: %s:%s" % (task_type, task_id))
                else:
                    chunk, t = future.result()
                    logger.info("Bulk uploaded a chunk of length %s, took %0.1fs" % (len(chunk), t))
                    # tot_uploaded += size
                    if tot_size > 0:
                        logger.info("%.1f%%" % (100 * (size_accu / tot_size)))
                        logger.info("------------------")


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
        queue = manager.Queue(maxsize=config.concurrency_queue_size)
        process(queue, insert_data, task_data, {}, tot_size, limit_to)
        upload_executor(insert_data, queue, tot_size, len(task_data))

    logger.info(index + " pipeline complete, took %i min and %i sec. " % divmod(time.time() - t, 60))
