import elasticsearch
import strix.config as config
from elasticsearch_dsl import Search, Q
from elasticsearch.exceptions import RequestError, NotFoundError

es = elasticsearch.Elasticsearch(config.elastic_hosts)


def create_query(search_type, search_field=None, search_term=None):
    if search_type in ["match", "wildcard", "match_phrase", "term"]:
        return Q(search_type, **{search_field: search_term})
    elif search_type == "match_all":
        return Q()
    else:
        raise ValueError("Not yet")


def search(indices, doc_type, search_type="match_all", search_field=None, search_term=None , includes=[], excludes=[], from_hit=0, to_hit=10, highlight=False):
    query = create_query(search_type, search_field, search_term)
    return search_query(indices, doc_type, query, includes=includes, excludes=excludes, from_hit=from_hit, to_hit=to_hit, highlight=highlight)


def search_query(indices, doc_type, query, includes=[], excludes=[], from_hit=0, to_hit=10, highlight=False):
    # how to exclude for example vivill
    # s = Search(using=es, index='litteraturbanken,-*vivill', doc_type=doc_type) \
    s = Search(using=es, index=indices, doc_type=doc_type).query(query)

    if highlight:
        # TODO pass what to highlight as parameter or something
        query_dict = query.to_dict()
        search_type = list(query_dict.keys())[0]
        if search_type in ["match", "match_phrase"]:
            s = s.highlight(list(query_dict[search_type].keys())[0], fragment_size=18)

    s = s.source(include=includes, exclude=excludes)
    hits = s[from_hit:to_hit].execute()

    items = []
    for hit in hits:
        item = hit.to_dict()
        if hasattr(hit.meta, 'highlight'):
            item['highlight'] = hit.meta.highlight.to_dict()
        item['es_id'] = hit.meta.id
        item['doc_type'] = hit.meta.doc_type
        items.append(item)
    return {"hits": hits.hits.total, "data": items}


def get_document_by_id(indices, doc_type, doc_id, includes, excludes):
    # TODO possible to fetch with document ID with DSL?
    try:
        result = es.get(index=indices, doc_type=doc_type, id=doc_id, _source_include=includes, _source_exclude=excludes)
    except NotFoundError:
        return None
    return {"data": result['_source']}


# TODO move all renaming stuff back to LB-api
def get_documents(indices, doc_type, from_hit, to_hit, includes=[], excludes=[], sort_field=None):
    s = Search(using=es, index=indices, doc_type=doc_type)
    s = s.source(include=includes, exclude=excludes)
    if sort_field:
        s = s.sort(sort_field)

    try:
        hits = s[from_hit:to_hit].execute()
    except RequestError as e:
        reason = e.info['error']['root_cause'][0]['reason']
        return {"error": reason}

    items = []
    for hit in hits:
        hit['es_id'] = hit.meta.id
        hit_dict = hit.to_dict()
        if hit_dict.get('meta', False):
            del hit_dict['meta']
        items.append(hit_dict)
    return {"hits": hits.hits.total, "data": items}
