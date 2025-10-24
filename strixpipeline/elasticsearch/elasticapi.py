import logging

from elasticsearch_dsl import Index

_logger = logging.getLogger(__name__)


def get_index_from_alias(es, alias_name):
    response = es.options(ignore_status=[400, 404]).indices.get_alias(name=alias_name)
    if "status" in response:
        return None
    return list(response.keys())[0]


def setup_alias(es, alias_name, new_index_name):
    old_index = get_index_from_alias(es, alias_name)
    if old_index:
        es.indices.delete_alias(name=alias_name, index=old_index)
    es.indices.put_alias(index=new_index_name, name=alias_name)


def delete_index_by_corpus_id(es, corpus):
    main_index = get_index_from_alias(es, corpus)
    if main_index:
        es.options(ignore_status=[400, 404]).indices.delete(index=main_index)
    term_index = get_index_from_alias(es, f"{corpus}_terms")
    if term_index:
        es.options(ignore_status=[400, 404]).indices.delete(index=term_index)


def create_index(es, index_name):
    index = Index(index_name, using=es)
    index.delete(ignore=404)
    index.create()


def close_index(es, index_name):
    es.cluster.health(index=index_name, wait_for_status="yellow")
    es.indices.close(index=index_name)


def open_index(es, index_name):
    es.indices.open(index=index_name)


def enable_insert_settings(es, alias, index_name=None):
    # set refresh_interval to -1 to speed up indexing
    set_refresh_interval(es, alias, -1, index_name=index_name)


def enable_postinsert_settings(es, alias, no_replicas, index_name=None):
    es.indices.put_settings(
        index=index_name or alias,
        body={
            "index.number_of_replicas": no_replicas,
        },
    )

    es.indices.put_settings(
        index=alias + "_terms",
        body={
            "index.number_of_replicas": no_replicas,
        },
    )
    es.indices.forcemerge(index=(index_name or alias) + "," + alias + "_terms")
    set_refresh_interval(es, alias, "1s", index_name=index_name)
    set_refresh_interval(es, alias, -1, index_name=index_name)


# TODO kräver faktiskt index namn??
def set_refresh_interval(es, alias, interval, index_name=None):
    es.indices.put_settings(
        index=(index_name or alias) + "," + alias + "_terms",
        body={
            "index.refresh_interval": interval,
        },
    )
