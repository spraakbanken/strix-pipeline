import glob
import json
import os

import elasticsearch
import requests

from elasticsearch_dsl import Search, Q
from elasticsearch.exceptions import NotFoundError

from strix import config

ALL_BUCKETS = "2147483647"

es = elasticsearch.Elasticsearch(config.elastic_hosts, timeout=120)


def search(indices, doc_type, field=None, search_term=None, includes=(), excludes=(), from_hit=0, to_hit=10, highlight=None, text_filter=None):
    if search_term:
        if field:
            query = Q("span_term", **{"text." + field: search_term})
        else:
            query = analyze_and_create_span_query(search_term)
    else:
        query = None
        highlight = None

    query = join_queries(text_filter, query)

    if search_term:
        query = Q("bool", should=[query, Q("fuzzy", title={"value": search_term, "boost": 50})])

    res = do_search_query(indices, doc_type, search_query=query, includes=includes, excludes=excludes, from_hit=from_hit, to_hit=to_hit, highlight=highlight)
    if "token_lookup" in includes or ("token_lookup" not in excludes and not includes):
        for document in res["data"]:
            document["token_lookup"] = get_terms(indices, doc_type, document["es_id"])

    return res


def do_search_query(indices, doc_type, search_query=None, includes=(), excludes=(), from_hit=0, to_hit=10, highlight=None, sort_field=None, before_send=None):
    s = get_search_query(indices, doc_type, search_query, includes=includes, excludes=excludes, from_hit=from_hit, to_hit=to_hit, highlight=highlight, sort_field=sort_field)
    if before_send:
        s = before_send(s)
    hits = s.execute()
    items = []
    for hit in hits:
        item = hit.to_dict()
        if highlight:
            item["highlight"] = process_hit(hit.meta.index, hit, 5)
        item["es_id"] = hit.meta.id
        item["doc_type"] = hit.meta.doc_type
        item["corpus"] = hit.meta.index
        move_text_attributes(hit.meta.index, item)
        items.append(item)

    output = {"hits": hits.hits.total, "data": items}
    if 'suggest' in hits:
        output['suggest'] = list(hits.to_dict()['suggest'].values())[0][0]["options"]
    if 'aggregations' in hits:
        output["aggregations"] = list(hits.to_dict()["aggregations"].values())[0]
    return output


def join_queries(text_filter, search_query):
    filter_clauses = []
    if text_filter:
        for k, v in text_filter.items():
            filter_clauses.append(Q("term", **{k: v}))

    if search_query and filter_clauses:
        filter_clauses.append(search_query)

    if filter_clauses:
        return Q("bool", must=filter_clauses)
    else:
        return search_query


def get_search_query(indices, doc_type, query=None, includes=(), excludes=(), from_hit=0, to_hit=10, highlight=None, sort_field=None):
    s = Search(index=indices, doc_type=doc_type)
    if query:
        s = s.query(query)

    if highlight:
        s = s.highlight('strix', options={"number_of_fragments": highlight["number_of_fragments"]})

    s = s.source(includes=includes, excludes=excludes)
    if sort_field:
        s = s.sort(sort_field)
    return s[from_hit:to_hit]


def get_document_by_id(indices, doc_type, doc_id, includes, excludes):
    # TODO possible to fetch with document ID with DSL?
    try:
        result = es.get(index=indices, doc_type=doc_type, id=doc_id, _source_include=includes, _source_exclude=excludes)
    except NotFoundError:
        return None

    document = result['_source']
    if "token_lookup" in includes or "token_lookup" not in excludes:
        document["token_lookup"] = get_terms(indices, doc_type, doc_id)
    document["es_id"] = result["_id"]
    document["corpus"] = result["_index"]
    move_text_attributes(result["_index"], document)
    return {"data": document}


def move_text_attributes(corpus, item):
    item["text_attributes"] = {}
    for text_attribute in text_attributes[corpus]:
        if text_attribute in item:
            item["text_attributes"][text_attribute] = item[text_attribute]
            del item[text_attribute]


def put_document(index, doc_type, doc):
    return es.index(index=index, doc_type=doc_type, body=doc)


def process_hit(index, hit, context_size):
    """
    takes a hit and extracts positions from highlighting and extracts
    tokens + attributes using termvectors (to be replaced with something more effective)
    :param hit: a non-parsed hit that has used the strix-highlighting
    :return: hit-element with added highlighting
    """
    es_id = hit.meta.id
    doc_type = hit.meta.doc_type

    if hasattr(hit.meta, "highlight"):
        highlights = get_highlights(index, es_id, doc_type, hit.meta.highlight.positions, context_size)
    else:
        highlights = []

    return {
        "highlight": highlights,
        "total_doc_highlights": len(highlights),
        "es_id": es_id
    }


def get_highlights(index, es_id, doc_type, spans, context_size):
    term_index = get_term_index(index, es_id, doc_type, spans, context_size)

    highlights = []

    seen_spans = []
    for span in spans:
        [_from, _to] = span.split('-')
        left = []
        match = []
        right = []

        from_int = int(_from)
        to_int = int(_to)
        # TODO possibly this will create problems due to overlapping spans
        if from_int in seen_spans:
            continue
        seen_spans.append(from_int)
        for pos in range(from_int - context_size, from_int):
            if pos in term_index:
                left.append(term_index[pos])
        for pos in range(from_int, to_int):
            match.append(term_index[pos])
        for pos in range(to_int, to_int + context_size):
            if pos in term_index:
                right.append(term_index[pos])
        highlights.append({"left_context": left, "match": match, "right_context": right})
    return highlights


def get_term_index(index, es_id, doc_type, spans, context_size):
    positions = set()
    for span in spans:
        [_from, _to] = span.split('-')
        from_int = int(_from)
        to_int = int(_to)

        positions.update(set(range(from_int, to_int)))

        if context_size > 0:
            positions.update(set(range(from_int - context_size, from_int)))
            positions.update(set(range(to_int, to_int + context_size)))

    return get_terms(index, doc_type, es_id, positions=list(positions))


def get_terms(index, doc_type, es_id, positions=(), from_pos=None, size=10):
    term_index = {}

    must_clauses = []
    if positions:
        must_clauses.append(Q('constant_score', filter=Q('terms', position=positions)))
    elif from_pos is not None:
        must_clauses.append(Q('constant_score', filter=Q('range', position={"gte": from_pos, "lte": from_pos + size - 1})))

    must_clauses.append(Q('term', doc_id=es_id))
    must_clauses.append(Q('term', doc_type=doc_type))

    query = Q('bool', must=must_clauses)

    s = Search(index=index + "_terms", doc_type="term").query(query)
    s.sort("_doc")

    for hit in s.scan():
        source = hit.to_dict()
        term_index[source['position']] = source['term']

    return term_index


def analyze_and_create_span_query(search_term, word_form_only=False):
    tokens = []
    for term in search_term.split(" "):
        words = []
        lemgrams = []
        if word_form_only or ("*" in term):
            words.append(term)
        else:
            lemgrams.extend(lemgrammify(term))
            if not lemgrams:
                words.append(term)
        tokens.append({"lemgram": lemgrams, "word": words})
    return create_span_query(tokens)


def lemgrammify(term):
    lemgrams = []
    response = requests.get("https://ws.spraakbanken.gu.se/ws/karp/v2/autocomplete?q=" + term + "&resource=saldom")
    result = response.json()
    for hit in result["hits"]["hits"]:
        lemgram = hit["_source"]["FormRepresentations"][0]["lemgram"]
        if "_" not in lemgram:
            lemgrams.append(lemgram)
    return lemgrams


def get_values(corpus, doc_type, field):
    s = Search(index=corpus, doc_type=doc_type)
    s.aggs.bucket("values", "terms", field=field, size=ALL_BUCKETS)
    result = s.execute()
    return result.aggregations.values.to_dict()["buckets"]


def search_in_document(corpus, doc_type, doc_id, field, value, current_position=-1, size=None, forward=True):
    s = Search(index=corpus, doc_type=doc_type)
    id_query = Q("term", _id=doc_id)
    span_query = Q("span_term", **{"text." + field: value})
    query = Q("bool", must=[id_query, span_query])
    s = s.query(query)
    s = s.source(excludes=["*"])
    s = s.highlight("strix")
    result = s.execute()
    for hit in result:
        obj = {
            "doc_id": doc_id,
            "doc_type": doc_type,
            "token_lookup": []
        }
        count = 0
        positions = hit.meta.highlight.positions
        if not forward:
            positions.reverse()
        for span_pos in positions:
            pos = int(span_pos.split("-")[0])
            if forward and pos > current_position or not forward and pos < current_position:
                terms = get_terms(corpus, doc_type, doc_id, positions=[pos])
                obj["token_lookup"].append(list(terms.values())[0])
                count += 1

            if size and size <= count:
                break

        if not forward:
            obj["token_lookup"].reverse()

        return obj

    return {}


# TODO support searching in any field and multiple fields per token (extended search style)
# assumes searching in field text
def create_span_query(tokens):
    span_terms = []
    for token_dict in tokens:
        if "lemgram" in token_dict and token_dict["lemgram"]:
            lemgram_terms = []
            for lemgram in token_dict["lemgram"]:
                lemgram_terms.append(Q("span_term", **{"text.lemgram": lemgram.lower()}))
            if len(lemgram_terms) > 1:
                query = Q("span_or", clauses=lemgram_terms)
            else:
                query = lemgram_terms[0]
            span_terms.append(query)
        elif "word" in token_dict and token_dict["word"]:
            word = token_dict["word"][0] # assume one word for now
            if '*' in word:
                span_terms.append(Q("span_multi", match={"wildcard": {"text": {"value": word}}}))
            else:
                span_terms.append(Q("span_term", **{"text": word}))
        else:
            raise RuntimeError("only non-empty tokens allowed")

    if len(span_terms) > 1:
        query = Q("span_near", clauses=span_terms, in_order=True, slop=0)
    else:
        query = span_terms[0]
    return query


def span_and(queries):
    # TODO make this work for all queries in lsit
    return Q("span_containing", big=queries[0], little=queries[1])


def mask_field(query, field="text"):
    return Q("field_masking_span", query=query, field=field)


def get_text_attributes():
    text_attributes = {}

    for file in glob.glob("resources/config/*.json"):
        key = os.path.splitext(os.path.basename(file))[0]
        text_attributes[key] = json.load(open(file, "r"))["analyze_config"]["text_attributes"]
        if "title" in text_attributes[key]:
            text_attributes[key].remove("title")

    text_attributes["litteraturbanken"] = []

    return text_attributes

text_attributes = get_text_attributes()
