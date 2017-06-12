# -*- coding: utf-8 -*-
import logging

import elasticsearch
import requests

from elasticsearch_dsl import Search, Q
from elasticsearch.exceptions import NotFoundError

from strix.config import config
import strix.corpusconf as corpusconf

ALL_BUCKETS = "2147483647"

es = elasticsearch.Elasticsearch(config.elastic_hosts, timeout=120)
_logger = logging.getLogger(__name__)


def search(doc_type, corpora=(), text_query_field=None, text_query=None, includes=(), excludes=(), from_hit=0, to_hit=10, highlight=None, text_filter=None, simple_highlight=False, token_lookup_from=None, token_lookup_to=None):
    simple_highlight_type = None
    add_fuzzy_query = False
    if text_query:
        if text_query_field:
            query = Q("span_term", **{"text." + text_query_field: text_query})
        else:
            query, simple_highlight_type = analyze_and_create_span_query(text_query, term_query=simple_highlight)
            add_fuzzy_query = True
    else:
        query = None
        highlight = None

    if add_fuzzy_query:
        query = Q("bool", should=[query, Q("fuzzy", title={"value": text_query, "boost": 50})])
    query = join_queries(text_filter, query)

    def before_send(s):
        if should_include("aggregations", includes, excludes):
            for index in corpora:
                for text_attribute, value in text_attributes[index].items():
                    if value.get("include_in_aggregation"):
                        s.aggs.bucket(text_attribute, "terms", field=text_attribute, size=ALL_BUCKETS, order={"_term": "asc"})
            s.aggs.bucket("corpora", "terms", field="_index", size=ALL_BUCKETS, order={"_term": "asc"})
        return s

    res = do_search_query(corpora, doc_type, search_query=query, includes=includes, excludes=excludes, from_hit=from_hit, to_hit=to_hit, highlight=highlight, simple_highlight=simple_highlight, simple_highlight_type=simple_highlight_type, before_send=before_send)

    if "aggregations" in res:
        corpora_buckets = []
        for bucket in res["aggregations"]["corpora"]["buckets"]:
            corpora_buckets.append({"doc_count": bucket["doc_count"], "key": bucket["key"].split("_")[0]})
        res["aggregations"]["corpora"]["buckets"] = corpora_buckets

    if token_lookup_from is not None and token_lookup_to is not None:
        for document in res["data"]:
            get_token_lookup(document, document["corpus"], doc_type, document["doc_id"], includes, excludes, token_lookup_from, token_lookup_to)

    return res


def get_related_documents(corpus, doc_type, doc_id, search_corpora=None, relevance_function="more_like_this", min_term_freq=1, max_query_terms=30, includes=(), excludes=(), from_hit=0, to_hit=10, token_lookup_from=None, token_lookup_to=None):
    if relevance_function == "more_like_this":
        query = Q("more_like_this",
                  fields=["similarity_tags"],
                  like=[{"_index": corpus, "_type": doc_type, "_id": doc_id}],
                  min_term_freq=min_term_freq,
                  max_query_terms=max_query_terms)
    else:
        s = Search(index=corpus, doc_type=doc_type)
        s = s.query(Q("term", _id=doc_id))
        s = s.source(includes="similarity_tags")
        hits = s.execute()
        if hits:
            for hit in s.execute():
                similarity_tags = hit.similarity_tags
                break
        else:
            raise RuntimeError("No document with ID " + doc_id)
        shoulds = []
        for tag in similarity_tags.split(" "):
            shoulds.append(Q("term", similarity_tags=tag))
        query = Q("bool", should=shoulds, must_not=Q("term", _id=doc_id))

    res = do_search_query(search_corpora if search_corpora else corpus, doc_type, search_query=query, includes=includes, excludes=excludes, from_hit=from_hit, to_hit=to_hit)
    if token_lookup_from is not None and token_lookup_to is not None:
        for document in res["data"]:
            get_token_lookup(document, corpus, doc_type, document["doc_id"], includes, excludes, token_lookup_from, token_lookup_to)

    return res


def do_search_query(corpora, doc_type, search_query=None, includes=(), excludes=(), from_hit=0, to_hit=10, highlight=None, simple_highlight=None, simple_highlight_type=None, sort_field=None, before_send=None):
    if to_hit > 10000:
        raise RuntimeError("Paging error. \"to\" cannot be larger than 10000")
    if to_hit < from_hit:
        raise RuntimeError("Paging error. \"to\" cannot be smaller than \"from\"")
    s = get_search_query(corpora, doc_type, search_query, includes=includes, excludes=excludes, from_hit=from_hit, to_hit=to_hit, highlight=highlight, simple_highlight=simple_highlight, simple_highlight_type=simple_highlight_type, sort_fields=sort_field)

    if before_send:
        s = before_send(s)

    hits = s.execute()
    items = []
    for hit in hits:
        hit_corpus = hit.meta.index.split("_")[0]
        item = hit.to_dict()
        if simple_highlight:
            if hasattr(hit.meta, "highlight"):
                item["highlight"] = process_simple_highlight(hit.meta.highlight)
        elif highlight:
            item["highlight"] = process_hit(hit_corpus, hit, 5)

        item["doc_id"] = hit.meta.id
        item["doc_type"] = hit.meta.doc_type
        item["corpus"] = hit_corpus
        move_text_attributes(hit_corpus, item, includes, excludes)
        items.append(item)

    output = {"hits": hits.hits.total, "data": items}
    if 'suggest' in hits:
        output['suggest'] = list(hits.to_dict()['suggest'].values())[0][0]["options"]
    if 'aggregations' in hits:
        output["aggregations"] = hits.to_dict()["aggregations"]
    return output


def join_queries(text_filter, search_query):
    filter_clauses = []
    if text_filter:
        for k, v in text_filter.items():
            if isinstance(v, str):
                filter_clauses.append(Q("term", **{k: v}))
            elif "range" in v:
                query_obj = v["range"]
                for key in query_obj.keys():
                    if key not in ["gte", "gt", "lte", "lt"]:
                        raise ValueError("Operator: " + key + " not supported by range query")
                filter_clauses.append(Q("range", **{k: v["range"]}))
            else:
                raise ValueError("Expression " + str(v) + " is not allowed")

    if search_query and filter_clauses:
        filter_clauses.append(search_query)

    if filter_clauses:
        return Q("bool", must=filter_clauses)
    else:
        return search_query


def get_search_query(indices, doc_type, query=None, includes=(), excludes=(), from_hit=0, to_hit=10, highlight=None, simple_highlight=None, simple_highlight_type=None, sort_fields=None):
    s = Search(index=indices, doc_type=doc_type)
    if query:
        s = s.query(query)

    if highlight:
        s = s.highlight('strix', options={"number_of_fragments": highlight["number_of_fragments"]})

    if simple_highlight:
        s = s.highlight("text.lemgram", type=simple_highlight_type or "plain", fragment_size=2500)

    excludes += ("text", "original_file", "similarity_tags")

    s = s.source(includes=includes, excludes=excludes)
    if isinstance(sort_fields, (list, tuple)):
        s = s.sort(*sort_fields)
    elif sort_fields:
        s = s.sort(sort_fields)
    return s[from_hit:to_hit]


def get_document_by_id(indices, doc_type, doc_id, includes=(), excludes=(), token_lookup_from=None, token_lookup_to=None):
    # TODO possible to fetch with document ID with DSL?
    try:
        if not excludes:
            excludes = []
        excludes += ("text", "original_file", "similarity_tags")
        result = es.get(index=indices, doc_type=doc_type, id=doc_id, _source_include=includes, _source_exclude=excludes)
    except NotFoundError:
        return None

    document = result['_source']
    document["doc_id"] = result["_id"]
    get_token_lookup(document, indices, doc_type, document["doc_id"], includes, excludes, token_lookup_from, token_lookup_to)
    hit_corpus = result["_index"].split("_")[0]
    document["corpus"] = hit_corpus

    move_text_attributes(hit_corpus, document, includes, excludes)
    return {"data": document}


def move_text_attributes(corpus, item, includes, excludes):
    if not should_include("text_attributes", includes, excludes):
        return
    if corpus not in text_attributes:
        return

    item["text_attributes"] = {}
    for text_attribute in text_attributes[corpus].keys():
        if text_attribute in item:
            item["text_attributes"][text_attribute] = item[text_attribute]
            del item[text_attribute]


def put_document(index, doc_type, doc):
    return es.index(index=index, doc_type=doc_type, body=doc)


def process_hit(corpus, hit, context_size):
    """
    takes a hit and extracts positions from highlighting and extracts
    tokens + attributes using termvectors (to be replaced with something more effective)
    :param hit: a non-parsed hit that has used the strix-highlighting
    :return: hit-element with added highlighting
    """
    doc_id = hit.meta.id
    doc_type = hit.meta.doc_type

    if hasattr(hit.meta, "highlight"):
        highlights = get_highlights(corpus, doc_id, doc_type, hit.meta.highlight.positions, context_size)
    else:
        highlights = []

    return {
        "highlight": highlights,
        "total_doc_highlights": len(highlights),
        "doc_id": doc_id
    }


def process_simple_highlight(highlights):
    result = []
    highlights = highlights["text.lemgram"]
    for highlight in highlights:
        sub_result = []
        for token in highlight.split("\u241D")[1:-1]:
            if token.startswith("<em>"):
                sub_result.append("<em>" + token[4:-5].split("\u241E")[0] + "</em>")
            else:
                sub_result.append(token.split("\u241E")[0])

        result.append(" ".join(sub_result))
    return {
        "highlight": result
    }


def get_highlights(corpus, doc_id, doc_type, spans, context_size):
    term_index = get_term_index(corpus, doc_id, doc_type, spans, context_size)

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


def get_term_index(corpus, doc_id, doc_type, spans, context_size):
    positions = set()
    for span in spans:
        [_from, _to] = span.split('-')
        from_int = int(_from)
        to_int = int(_to)

        positions.update(set(range(from_int, to_int)))

        if context_size > 0:
            positions.update(set(range(from_int - context_size, from_int)))
            positions.update(set(range(to_int, to_int + context_size)))

    return get_terms(corpus, doc_type, doc_id, positions=list(positions))


def get_terms(corpus, doc_type, doc_id, positions=(), from_pos=None, size=None):
    term_index = {}

    must_clauses = []
    if positions:
        must_clauses.append(Q('constant_score', filter=Q('terms', position=positions)))
    elif from_pos or size:
        if not from_pos:
            from_pos = 0
        position_range = {"gte": from_pos}
        if size:
            position_range["lte"] = from_pos + size - 1
        must_clauses.append(Q('constant_score', filter=Q('range', position=position_range)))

    must_clauses.append(Q('term', doc_id=doc_id))
    must_clauses.append(Q('term', doc_type=doc_type))

    query = Q('bool', must=must_clauses)

    s = Search(index=corpus + "_terms", doc_type="term").query(query)
    s.sort("_doc")

    for hit in s.scan():
        source = hit.to_dict()
        term_index[source['position']] = source['term']

    return term_index


def analyze_and_create_span_query(search_term, word_form_only=False, term_query=False):
    tokens = []
    terms = search_term.split(" ")
    if not term_query or len(terms) > 1:
        for term in terms:
            words = []
            lemgrams = []
            if word_form_only or ("*" in term):
                words.append(term)
            else:
                lemgrams.extend(lemgrammify(term))
                if not lemgrams:
                    words.append(term)
            tokens.append({"lemgram": lemgrams, "word": words})
        return create_span_query(tokens), "plain"
    else:
        term = terms[0]
        should = []
        for lemgram in lemgrammify(term):
            should.append(Q("term", **{"text.lemgram": lemgram}))
        return Q("bool", should=should), "fvh"


def lemgrammify(term):
    lemgrams = []
    response = requests.get("https://ws.spraakbanken.gu.se/ws/karp/v2/autocomplete?q=" + term + "&resource=saldom")
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        _logger.exception("Unable to use Karp autocomplete service")
        raise RuntimeError("Unable to use Karp autocomplete service")
    result = response.json()
    for hit in result["hits"]["hits"]:
        lemgram = hit["_source"]["FormRepresentations"][0]["lemgram"]
        if "_" not in lemgram:
            lemgrams.append(lemgram.lower())  # .lower() here is because we accidentally have lowercase active in the mapping
    return lemgrams


def get_values(corpus, doc_type, field):
    s = Search(index=corpus, doc_type=doc_type)
    s.aggs.bucket("values", "terms", field=field, size=ALL_BUCKETS)
    result = s.execute()
    return result.aggregations.values.to_dict()["buckets"]


def search_in_document(corpus, doc_type, doc_id, current_position=-1, size=None, forward=True, text_query=None, text_query_field=None, includes=(), excludes=(), token_lookup_from=None, token_lookup_to=None):
    s = Search(index=corpus, doc_type=doc_type)
    id_query = Q("term", _id=doc_id)
    if text_query_field and text_query:
        span_query = Q("span_term", **{"text." + text_query_field: text_query})
    elif text_query:
        span_query, _ = analyze_and_create_span_query(text_query)
    else:
        span_query = None
    if span_query:
        should = [span_query]
    else:
        should = []
    query = Q("bool", must=[id_query], should=should)
    s = s.query(query)

    excludes += ("text", "original_file", "similarity_tags")

    s = s.source(includes=includes, excludes=excludes)
    s = s.highlight("strix")
    result = s.execute()
    for hit in result:
        obj = hit.to_dict()
        obj["doc_id"] = hit.meta.id
        obj["doc_type"] = hit.meta.doc_type
        obj["corpus"] = hit.meta.index.split("_")[0]
        obj["highlight"] = []

        move_text_attributes(obj["corpus"], obj, includes, excludes)

        if size != 0 and hasattr(hit.meta, "highlight"):
            count = 0
            positions = hit.meta.highlight.positions
            if not forward:
                positions.reverse()

            for span_pos in positions:
                pos = int(span_pos.split("-")[0])
                if forward and pos > current_position or not forward and pos < current_position:
                    terms = get_terms(corpus, doc_type, doc_id, positions=[pos])
                    obj["highlight"].append(list(terms.values())[0])
                    count += 1

                if size and size <= count:
                    break

        if not forward:
            obj["highlight"].reverse()

        get_token_lookup(obj, corpus, doc_type, obj["doc_id"], includes, excludes, token_lookup_from, token_lookup_to)

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
            word = token_dict["word"][0]  # Assume one word for now
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


def get_token_lookup(document, corpus, doc_type, doc_id, includes, excludes, token_lookup_from, token_lookup_to):
    if should_include("token_lookup", includes, excludes):
        kwargs = {}
        if token_lookup_from:
            kwargs["from_pos"] = token_lookup_from
        if token_lookup_to:
            kwargs["size"] = token_lookup_to - token_lookup_from
        document["token_lookup"] = get_terms(corpus, doc_type, doc_id, **kwargs)


def should_include(attribute, includes, excludes):
    if includes:
        return attribute in includes
    else:
        return attribute not in excludes and "*" not in excludes


def get_config(only_ids=False):
    # TODO replace this with all aliases
    result = es.cat.indices(h="index", index="*_terms")
    index_names = []
    for term_index in result.split("\n"):
        if not term_index:
            continue
        index_names.append(term_index.split("_terms")[0])

    if only_ids:
        return index_names

    indices = {}
    for index in index_names:
        config_json = corpusconf.get_corpus_conf(index)
        names = config_json["corpus_name"]
        descriptions = config_json.get("corpus_description")
        analyze_config = config_json["analyze_config"]
        indices[index] = {
            "name": names,
            "description": descriptions,
            "attributes": analyze_config
        }
    return indices


def get_all_corpora_ids():
    return get_config(only_ids=True)


text_attributes = corpusconf.get_text_attributes()


def parse_date_range_params(params, date_field):
    if "from" in params:
        date_from = params.get("from")
        date_to = params.get("to", "now")

        date_range = Q("range", **{date_field: {
            "from": date_from,
            "to": date_to
        }})
    else:
        date_range = Q()

    return date_range


def date_histogram(index, doc_type, field, params):
    def add_aggs(search):
        a = search.aggs.bucket("histogram", "date_histogram", field=date_field, interval="year")
        a.bucket("word_count", "sum", field="word_count")
        a.bucket(field, "terms", field=field)
        return search

    date_field = params.get("date_field")
    date_range = parse_date_range_params(params, date_field)

    response = do_search_query(index,
                               doc_type,
                               date_range & Q("exists", field=date_field) & Q("exists", field="text"),
                               to_hit=0,
                               before_send=add_aggs)

    output = []
    for item in response["aggregations"]["histogram"]["buckets"]:
        x = item["key"]
        y = item["word_count"]["value"]
        titles = [x["key"] for x in item[field]["buckets"]]
        output.append({"x": x / 1000, "y": y, "titles": titles})

    return output
