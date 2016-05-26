import strix.elasticapi as elasticapi
from elasticsearch_dsl import Q

INDEX_NAME = "litteraturbanken"

rename_mapping = {
    "full_name": "fullname",
    "name_for_index": "nameforindex",
    "author_id": "authorid",
}

inv_rename_mapping = {v: k for k, v in rename_mapping.items()}


def to_elastic_names(includes, excludes):
    includes = [inv_rename_mapping.get(include, include) for include in includes]
    excludes = [inv_rename_mapping.get(exclude, exclude) for exclude in excludes]
    return includes, excludes


def from_elastic_names(result):
    for res in result:
        for k, v in rename_mapping.items():
            if res.get(k, False):
                res[v] = res[k]
                del res[k]


def get_document_by_id(doc_type, doc_id, includes, excludes):
    return elasticapi.get_document_by_id(INDEX_NAME, doc_type, doc_id, includes, excludes)


def get_work_by_field(field, _from=0, _to=10, includes=[], excludes=[]):
    query = Q('bool', filter=[Q('exists', field=field)])
    return elasticapi.search_query(INDEX_NAME, "etext", query, includes=includes, excludes=excludes, from_hit=_from, to_hit=_to)


def get_documents(doc_type, from_hit, to_hit, includes=[], excludes=[], sort_field=None):
    includes, excludes = to_elastic_names(includes, excludes)
    result = elasticapi.get_documents(INDEX_NAME, doc_type, from_hit, to_hit, includes, excludes, sort_field)
    from_elastic_names(result['data'])
    return result


def search_work_by_filter(doc_types, filter_string, from_hit, to_hit, includes=[], excludes=[]):
    query = Q("bool", should=[
        Q("nested", path="authors", query=
            Q("wildcard", **{"authors.full_name": "*" + filter_string + "*"})
        ),
        Q("wildcard", title="*" + filter_string + "*")
    ])

    return elasticapi.search_query(INDEX_NAME, doc_types, query, from_hit=from_hit, to_hit=to_hit, includes=includes, excludes=excludes)


def search_work_by_authors(doc_types, authors, from_hit, to_hit, includes=[], excludes=[]):
    sub_queries = []
    for auth in authors:
        sub_queries.append(Q("term", **{"authors.author_id": auth}))
    query = Q("nested", path="authors", query=Q("bool", should=sub_queries))

    return elasticapi.search_query(INDEX_NAME, doc_types, query, from_hit=from_hit, to_hit=to_hit, includes=includes, excludes=excludes)

def search_text(query_string):
    pass