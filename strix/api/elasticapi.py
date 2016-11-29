import elasticsearch
import requests

from elasticsearch_dsl import Search, Q
from elasticsearch.exceptions import RequestError, NotFoundError

from strix import config

es = elasticsearch.Elasticsearch(config.elastic_hosts, timeout=120)


def search(indices, doc_type, search_term=None, includes=(), excludes=(), from_hit=0, to_hit=10, highlight=None):
    query = analyze_and_create_span_query(search_term)
    res = search_query(indices, doc_type, query, includes=includes, excludes=excludes, from_hit=from_hit, to_hit=to_hit, highlight=highlight)
    for document in res["data"]:
        if "token_lookup" in includes or "token_lookup" not in excludes:
            document["token_lookup"] = get_terms(indices, doc_type, document["es_id"])
    return res


def search_query(indices, doc_type, query, includes=(), excludes=(), from_hit=0, to_hit=10, highlight=None, sort_field=None, before_send=None):
    s = get_search_query(indices, doc_type, query, includes=includes, excludes=excludes, from_hit=from_hit, to_hit=to_hit, highlight=highlight, sort_field=sort_field)
    if before_send:
        s = before_send(s)
    hits = s.execute()
    items = []
    for hit in hits:
        item = hit.to_dict()
        if highlight:
            item["highlight"] = process_hit(indices, hit, 5)
        item['es_id'] = hit.meta.id
        item['doc_type'] = hit.meta.doc_type
        items.append(item)

    output = {"hits": hits.hits.total, "data": items}
    if 'suggest' in hits:
        output['suggest'] = list(hits.to_dict()['suggest'].values())[0][0]["options"]
    if 'aggregations' in hits:
        output["aggregations"] = list(hits.to_dict()["aggregations"].values())[0]
    return output


def get_search_query(indices, doc_type, query, includes=(), excludes=(), from_hit=0, to_hit=10, highlight=None, sort_field=None):
    s = Search(index=indices, doc_type=doc_type).query(query)

    if highlight:
        s = s.highlight('strix', options={"number_of_fragments": highlight["number_of_fragments"]})

    s = s.source(include=includes, exclude=excludes)
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
    document['es_id'] = result['_id']
    return {"data": document}


def get_documents(indices, doc_type, from_hit, to_hit, includes=(), excludes=(), sort_field=None):
    s = Search(index=indices, doc_type=doc_type)
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
        hit["es_id"] = hit.meta.id
        hit_dict = hit.to_dict()
        if "token_lookup" in includes or "token_lookup" not in excludes:
            hit_dict["token_lookup"] = get_terms(indices, doc_type, hit.meta.id)
        if hit_dict.get("meta", False):
            del hit_dict["meta"]
        items.append(hit_dict)
    return {"hits": hits.hits.total, "data": items}


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
    highlights = hit.meta.highlight.positions
    highlights = get_highlights(index, es_id, doc_type, highlights, context_size)

    return {
        "highlight": highlights,
        "total_doc_highlights": len(highlights),
        "es_id": es_id
    }


def get_highlights(index, es_id, doc_type, spans, context_size):
    term_index = get_term_index(index, es_id, doc_type, spans, context_size)

    highlights = []

    for span in spans:
        [_from, _to] = span.split('-')
        left = []
        match = []
        right = []

        from_int = int(_from)
        to_int = int(_to)
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


# TODO support searching in any field and multiple fields per token (extended search style)
# assumes searching in field text
def create_span_query(tokens):
    span_terms = []
    for token_dict in tokens:
        if "lemgram" in token_dict and token_dict["lemgram"]:
            lemgram_terms = []
            for lemgram in token_dict["lemgram"]:
                lemgram_terms.append(Q("span_term", **{"text.lemgram": lemgram}))
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
