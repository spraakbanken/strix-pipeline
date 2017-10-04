# -*- coding: utf-8 -*-
import logging
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.connections import connections

from strix.config import config
import strix.corpusconf as corpusconf
import strix.api.karp as karp
from strix.api.elasticapihelpers import page_size
import strix.api.highlighting as highlighting

ALL_BUCKETS = "2147483647"

es = connections.create_connection(hosts=config.elastic_hosts if config.has_attr("elastic_hosts") else None, timeout=120)
_logger = logging.getLogger(__name__)


def search(doc_type, corpora=(), text_query=None, includes=(), excludes=(), size=None, highlight=None,
           text_filter=None, simple_highlight=False, token_lookup_size=None):
    query, use_highlight = get_search_query(text_query, text_filter)
    if not use_highlight:
        highlight = None

    res = do_search_query(corpora, doc_type, search_query=query, includes=includes, excludes=excludes, size=size, highlight=highlight, simple_highlight=simple_highlight)

    if token_lookup_size:
        for document in res["data"]:
            get_token_lookup(document, document["corpus"], doc_type, document["doc_id"], includes, excludes, token_lookup_size)

    return res


def get_related_documents(corpus, doc_type, doc_id, corpora=None, text_query=None, text_filter=None,
                          relevance_function="more_like_this", min_term_freq=1, max_query_terms=30, includes=(), excludes=(),
                          size=None, token_lookup_size=None):
    related_query = None
    s = Search(index=corpus, doc_type=doc_type)
    s = s.query(Q("term", doc_id=doc_id))

    if relevance_function == "more_like_this":
        s = s.source(False)
        hits = s.execute()
        es_id = [hit.meta.id for hit in hits][0]
        related_query = Q("more_like_this",
                          fields=["similarity_tags"],
                          like=[{"_index": corpus, "_type": doc_type, "_id": es_id}],
                          min_term_freq=min_term_freq,
                          max_query_terms=max_query_terms)
    else:
        s = s.source(includes="similarity_tags")
        hits = s.execute()
        if hits:
            for hit in s.execute():
                similarity_tags = hit.similarity_tags
                shoulds = []
                for tag in similarity_tags.split(" "):
                    shoulds.append(Q("term", similarity_tags=tag))
                related_query = Q("bool", should=shoulds, must_not=Q("term", _id=doc_id))
                break
        else:
            raise RuntimeError("No document with ID " + doc_id)

    if related_query:
        doc_query, _ = get_search_query(text_query, text_filter)
        if doc_query:
            related_query = Q("bool", must=[related_query], filter=[doc_query])

        res = do_search_query(corpora, doc_type, search_query=related_query, includes=includes, excludes=excludes, size=size)
        if token_lookup_size:
            for document in res["data"]:
                get_token_lookup(document, corpus, doc_type, document["doc_id"], includes, excludes, token_lookup_size)

        return res
    else:
        return {}


def get_search_query(text_query_obj, text_filter):
    if text_query_obj is None:
        text_query_obj = {}

    text_query_field = text_query_obj.get("text_query_field", None)
    include_alternatives = text_query_obj.get("include_alternatives", False)
    in_order = text_query_obj.get("in_order", True)
    text_query = text_query_obj.get("text_query", None)

    add_fuzzy_query = False
    search_queries = []
    if text_query:
        use_highlight = True
        if text_query_field:
            if include_alternatives and corpusconf.is_ranked(text_query_field):
                text_query_field = text_query_field + "_alt"
            search_queries.append(Q("span_term", **{"text." + text_query_field: text_query}))
        else:
            query = analyze_and_create_span_query(text_query, in_order=in_order)
            search_queries.append(query)
            add_fuzzy_query = True
    else:
        use_highlight = False

    if add_fuzzy_query:
        title_queries = []
        for term in tokenize_search_string(text_query):
            title_queries.append(Q("fuzzy", title={"value": term[0], "boost": 3}))
        search_queries.append(Q("bool", must=title_queries))
    return join_queries(text_filter, search_queries), use_highlight


def do_search_query(corpora, doc_type, search_query=None, includes=(), excludes=(), size=None, highlight=None,
                    simple_highlight=None, sort_field=None, before_send=None):

    if simple_highlight:
        highlight = {"number_of_fragments": 5}

    s = get_search(corpora, doc_type, search_query, includes=includes, excludes=excludes, size=size, highlight=highlight, sort_fields=sort_field)

    if before_send:
        s = before_send(s)

    hits = s.execute()
    highlight_documents = {}
    results = []
    for hit in hits:
        result = {}
        hit_corpus = corpus_id_to_alias(hit.meta.index)
        item = hit.to_dict()
        result["item"] = item

        if "doc_id" in item:
            item["doc_id"] = hit["doc_id"]
        else:
            item["doc_id"] = hit.meta.id
        item["doc_type"] = hit.meta.doc_type
        item["corpus_id"] = hit_corpus
        if highlight or simple_highlight:
            highlighting.get_spans_for_highlight(result, highlight_documents, hit_corpus, doc_type, item["doc_id"], hit)

        move_text_attributes(hit_corpus, item, includes, excludes)
        results.append(result)

    highlighting.highlight_search(highlight_documents, results, highlight=highlight, simple_highlight=simple_highlight)

    output = {"hits": hits.hits.total, "data": [res["item"] for res in results]}
    if "suggest" in hits:
        output["suggest"] = list(hits.to_dict()["suggest"].values())[0][0]["options"]
    if "aggregations" in hits:
        output["aggregations"] = hits.to_dict()["aggregations"]
    return output


def get_text_filters(text_filter):
    if not text_filter:
        return {}
    filter_clauses = {}
    for k, v in text_filter.items():
        attr = corpusconf.get_text_attribute(k)
        if isinstance(v, str):
            filter_clauses[k] = Q("term", **{k: v})
        elif attr.get("type", None) == "double":
            v = [v] if not isinstance(v, list) else v
            clauses = []
            for val in v:
                clauses.append(Q("range", **{k: {"lt": val + attr.get("interval", 20), "gte": val}}))
            filter_clauses[k] = Q("bool", should=clauses)
        elif isinstance(v, list):
            filter_clauses[k] = Q("terms", **{k: v})
        elif isinstance(v, dict) and "range" in v:
            query_obj = v["range"]
            for key in query_obj.keys():
                if key not in ["gte", "gt", "lte", "lt"]:
                    raise ValueError("Operator: " + key + " not supported by range query")
            filter_clauses[k] = Q("range", **{k: v["range"]})
        else:
            raise ValueError("Expression " + str(v) + " for key " + k + " is not allowed")
    return filter_clauses


def join_queries(text_filter, search_queries):
    filter_clauses = list(get_text_filters(text_filter).values())

    if filter_clauses:
        if len(search_queries) > 0:
            minimum_should_match = 1
        else:
            minimum_should_match = 0
        return Q("bool", filter=filter_clauses, should=search_queries, minimum_should_match=minimum_should_match)
    elif len(search_queries) > 1:
        return Q("bool", should=search_queries, minimum_should_match=1)
    elif len(search_queries) is 1:
        return search_queries[0]
    else:
        return None


def get_search(indices, doc_type, query=None, includes=(), excludes=(), size=None, highlight=None, sort_fields=None):
    s = Search(index=indices, doc_type=doc_type)
    if query:
        s = s.query(query)

    if highlight:
        s = s.highlight("strix", options={"number_of_fragments": highlight["number_of_fragments"]})

    new_includes, new_excludes = fix_includes_excludes(includes, excludes, indices)

    s = s.source(includes=new_includes, excludes=new_excludes)
    if isinstance(sort_fields, (list, tuple)):
        s = s.sort(*sort_fields)
    elif sort_fields:
        s = s.sort(sort_fields)
    if size:
        return s[size["from"]:size["to"]]
    else:
        return s


def get_document_by_id(corpus_id, doc_type, doc_id=None, sentence_id=None, includes=(), excludes=(), token_lookup_size=None):
    if not excludes:
        excludes = []
    excludes += ("text", "original_file", "similarity_tags")
    if includes:
        includes += ("doc_id", )
    if doc_id:
        query = Q("term", doc_id=doc_id)
    elif sentence_id:
        query = Q("term", **{"text.sentence_id": sentence_id})
    else:
        raise ValueError("Document id or or sentence id must be given")

    s = Search(index=corpus_id, doc_type=doc_type)
    new_includes, new_excludes = fix_includes_excludes(includes, excludes, corpus_id)
    s = s.source(includes=new_includes, excludes=new_excludes)
    s = s.query(query)
    s = s[0:1]
    result = s.execute()

    for hit in result:
        document = hit.to_dict()
        document["doc_id"] = hit["doc_id"]
        get_token_lookup(document, corpus_id, doc_type, document["doc_id"], includes, excludes, token_lookup_size)
        move_text_attributes(corpus_id, document, includes, excludes)
        return {"data": document}
    return {}


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


def analyze_and_create_span_query(search_term, word_form_only=False, in_order=True):
    tokens = []
    terms = tokenize_search_string(search_term)
    if not word_form_only:
        res = karp.lemgrammify([term[0] for term in terms if not (term[1] or "*" in term[0])])
    else:
        res = {}
    for term in terms:
        words = []
        lemgrams = []
        word = term[0]
        term_word_form_only = term[1]
        words.append(word)
        if (not word_form_only) and (not (term_word_form_only or ("*" in word))):
            lemgrams.extend(res[word])
        tokens.append({"lemgram": lemgrams, "word": words})
    if in_order:
        return create_span_query(tokens)
    else:
        return create_span_query_keyword(tokens)


def tokenize_search_string(search_term):
    terms = []
    # TODO this needs to be replaced with proper tokenizing
    split = search_term.split(" ")
    in_quotes = False
    for term in split:
        still_in_quotes = in_quotes
        if len(term) > 1:
            start_idx = 0
            end_idx = len(term)
            if term.startswith('"'):
                in_quotes = True
                still_in_quotes = True
                start_idx = 1
            if term.endswith('"'):
                end_idx = -1
                still_in_quotes = False
            term = term[start_idx:end_idx]

        terms.append([term, in_quotes])
        in_quotes = still_in_quotes
    return terms


def search_in_document(corpus, doc_type, doc_id, current_position=-1, size=None, forward=True, text_query=None,
                       includes=(), excludes=(), token_lookup_size=None):
    text_query_field = text_query.get("text_query_field", None)
    text_query_text = text_query.get("text_query", None)
    include_alternatives = text_query.get("include_alternatives", False)
    s = Search(index=corpus, doc_type=doc_type)
    id_query = Q("term", doc_id=doc_id)
    if text_query_field and text_query_text:
        if include_alternatives and corpusconf.is_ranked(text_query_field):
            text_query_field = text_query_field + "_alt"
        span_query = Q("span_term", **{"text." + text_query_field: text_query_text})
    elif text_query_text:
        span_query = analyze_and_create_span_query(text_query_text)
    else:
        span_query = None
    if span_query:
        should = [span_query]
    else:
        should = []
    query = Q("bool", must=[id_query], should=should)
    s = s.query(query)

    new_includes, new_excludes = fix_includes_excludes(includes, excludes, [corpus])

    s = s.source(includes=new_includes, excludes=new_excludes)
    s = s.highlight("strix")
    result = s.execute()
    for hit in result:
        obj = hit.to_dict()
        obj["doc_type"] = hit.meta.doc_type
        move_text_attributes(corpus, obj, includes, excludes)
        obj["highlight"] = highlighting.add_highlight_to_doc(corpus, doc_type, doc_id, hit, current_position=current_position, size=size, forward=forward)
        get_token_lookup(obj, corpus, doc_type, obj["doc_id"], includes, excludes, token_lookup_size)

        return obj

    return {}


def fix_includes_excludes(includes, excludes, corpora):
    new_includes = list(includes)
    new_excludes = list(excludes)

    if "*" in excludes:
        new_excludes = ("dump", "lines")
        for corpus in corpora:
            new_excludes += tuple(text_attributes[corpus].keys())
    if "*" in excludes or "text_attributes" in excludes:
        for corpus in corpora:
            new_excludes += tuple(text_attributes[corpus].keys())
    new_excludes += ("text", "original_file", "similarity_tags")

    if includes:
        includes += ("doc_id",)

    return new_includes, new_excludes


# TODO support searching in any field and multiple fields per token (extended search style)
# assumes searching in field text
def create_span_query(tokens):
    span_terms = []
    for token_dict in tokens:
        span_ors = []
        if token_dict["lemgram"]:
            for lemgram in token_dict["lemgram"]:
                span_ors.append(Q("span_term", **{"text.lemgram": lemgram.lower()}))
        if token_dict["word"]:
            for word in token_dict["word"]:
                if "*" in word:
                    span_ors.append(mask_field(Q("span_multi", match={"wildcard": {"text": {"value": word}}}), field="text.lemgram"))
                else:
                    span_ors.append(mask_field(Q("span_term", **{"text": word}), field="text.lemgram"))
        if len(span_ors) > 1:
            query = Q("span_or", clauses=span_ors)
        else:
            query = span_ors[0]
        span_terms.append(query)

    if len(span_terms) > 1:
        query = Q("span_near", clauses=span_terms, in_order=True, slop=0)
    else:
        query = span_terms[0]
    return query


def create_span_query_keyword(tokens):
    phrase_query = create_span_query(tokens)
    must_clauses = []
    for token_dict in tokens:
        or_clauses = []
        if token_dict["lemgram"]:
            for lemgram in token_dict["lemgram"]:
                or_clauses.append(Q("span_term", **{"text.lemgram": lemgram.lower()}))
        if token_dict["word"]:
            for word in token_dict["word"]:
                if "*" in word:
                    or_clauses.append(mask_field(Q("span_multi", match={"wildcard": {"text": {"value": word}}}), field="text.lemgram"))
                else:
                    or_clauses.append(mask_field(Q("span_term", **{"text": word}), field="text.lemgram"))
        if len(or_clauses) > 1:
            query = Q("bool", should=or_clauses, minimum_should_match=1)
        else:
            query = or_clauses[0]
        must_clauses.append(query)

    return Q("bool", should=[Q("bool", must=must_clauses), phrase_query])


def span_and(queries):
    # TODO make this work for all queries in lsit
    return Q("span_containing", big=queries[0], little=queries[1])


def mask_field(query, field="text"):
    return Q("field_masking_span", query=query, field=field)


def get_token_lookup(document, corpus, doc_type, doc_id, includes, excludes, token_lookup_size):
    if should_include("token_lookup", includes, excludes):
        kwargs = {}
        if token_lookup_size:
            from_ = token_lookup_size["from"]
            kwargs["from_pos"] = from_
            kwargs["size"] = token_lookup_size["to"] - from_
        document["token_lookup"] = highlighting.get_terms_for_doc(corpus, doc_type, doc_id, **kwargs)


def should_include(attribute, includes, excludes):
    if includes:
        return attribute in includes
    else:
        return attribute not in excludes and "*" not in excludes


def get_config(only_ids=False):
    result = es.cat.aliases(h="alias")
    index_names = []
    for index_name in result.split("\n")[:-1]:
        index_names.append(index_name)

    if only_ids:
        return index_names

    indices = {}
    for index in index_names:
        config_json = corpusconf.get_corpus_conf(index)
        names = config_json["corpus_name"]
        descriptions = config_json.get("corpus_description")
        analyze_config = config_json["analyze_config"]

        def update_translation(attr):
            translation_value = attr.get("translation_value", {}).get("-", None)
            if translation_value:
                attr["translation_value"]["swe"] = translation_value
                attr["translation_value"]["eng"] = translation_value
                del attr["translation_value"]["-"]
            return attr

        word_attrs = [update_translation(corpusconf.get_word_attribute(attr))for attr in analyze_config["word_attributes"]]
        text_attrs = [update_translation(corpusconf.get_text_attribute(attr)) for attr in analyze_config["text_attributes"]]
        struct_attrs = {}
        for struct_node, struct_config in analyze_config["struct_attributes"].items():
            struct_attrs[struct_node] = [update_translation(corpusconf.get_struct_attribute(attr)) for attr in struct_config]

        indices[index] = {
            "name": names,
            "description": descriptions,
            "attributes": {"word_attributes": word_attrs, "text_attributes": text_attrs, "struct_attributes": struct_attrs}
        }
    return indices


def get_all_corpora_ids():
    return get_config(only_ids=True)


text_attributes = corpusconf.get_text_attributes()


def get_most_common_text_attributes(corpora, facet_count, include_facets):
    if include_facets:
        facet_count = len(include_facets)
    supported_text_attributes = {}
    for index in corpora:
        for text_attribute, value in text_attributes[index].items():
            if value.get("include_in_aggregation"):
                if text_attribute in supported_text_attributes:
                    supported_text_attributes[text_attribute] = (supported_text_attributes[text_attribute][0] + 1, supported_text_attributes[text_attribute][1])
                else:
                    supported_text_attributes[text_attribute] = (1, value)

    def sort_facets(facets):
        tmp = sorted(facets.items(), key=lambda x: x[1][1]["name"])
        tmp = sorted(tmp, key=lambda x: x[1][0], reverse=True)
        return [(text_attribute, attr_type[1]) for (text_attribute, attr_type) in tmp]

    if include_facets:
        all_attributes = []
        for facet in include_facets:
            if facet in supported_text_attributes:
                all_attributes.append((facet, supported_text_attributes[facet][1]))
                del supported_text_attributes[facet]
            else:
                facet_count -= 1
        all_attributes.extend(sort_facets(supported_text_attributes))
    else:
        all_attributes = sort_facets(supported_text_attributes)
    return all_attributes[0:facet_count], [x[0] for x in all_attributes[facet_count:]]


def get_aggs(corpora=(), text_query=None, text_filter=None, facet_count=4, include_facets=(),
             min_doc_count=0):
    if len(corpora) == 0:
        raise ValueError("Something went wrong")

    text_filters = get_text_filters(text_filter)
    if "corpus_id" not in text_filters:
        text_filters["corpus_id"] = Q("terms", corpus_id=corpora)

    # first find out which corpora that will actually match the query
    corpora_search = Search(index=corpora, doc_type="text")
    corpora_search = corpora_search.query(Q("bool", filter=list(text_filters.values())))
    corpora_search.aggs.bucket("corpus_id", "terms", field="corpus_id", size=ALL_BUCKETS)
    corpora_search = corpora_search[0:0]
    doc_query, _ = get_search_query(text_query, text_filter)
    if doc_query:
        corpora_search = corpora_search.query(doc_query)
    result = corpora_search.execute().to_dict()
    hit_corpora = [bucket["key"] for bucket in result["aggregations"]["corpus_id"]["buckets"]]

    (use_text_attributes, additional_text_attributes) = get_most_common_text_attributes(hit_corpora, facet_count - 1, include_facets)
    use_text_attributes.append(("corpus_id", {"type": "keyword"}))

    s = Search(index="*", doc_type="text")
    doc_query, _ = get_search_query(text_query, {})
    date_aggs = []
    for (text_attribute, attr_settings) in use_text_attributes:
        filters = [value for text_filter, value in text_filters.items() if text_filter != text_attribute]
        if doc_query:
            filters.append(doc_query)
        a = s.aggs.bucket(text_attribute + "_all", "filter", filter=Q("bool", filter=filters))
        attr_type = attr_settings.get("type", "keyword")
        if attr_type == "date":
            a = a.bucket(text_attribute, "date_histogram", field=text_attribute, interval="year",  min_doc_count=min_doc_count)
            a.bucket("word_count", "sum", field="word_count")
            date_aggs.append(text_attribute)
        elif attr_type == "double":
            interval = attr_settings.get("interval", 20)
            if attr_settings.get("has_infinite", False):
                min_doc_count = 1
            a.bucket(text_attribute, "histogram", field=text_attribute, interval=interval, min_doc_count=min_doc_count)
        else:
            a.bucket(text_attribute, "terms", field=text_attribute, size=ALL_BUCKETS, order={"_term": "asc"}, min_doc_count=min_doc_count)

    s = s[0:0]
    result = s.execute().to_dict()

    new_result = {"aggregations": {}, "unused_facets": additional_text_attributes}
    for x in result["aggregations"]:
        new_key = x.split("_all")[0]
        new_result["aggregations"][new_key] = result["aggregations"][x][new_key]
        if new_key in date_aggs:
            for bucket in new_result["aggregations"][new_key]["buckets"]:
                del bucket["key_as_string"]
                bucket["word_count"] = bucket["word_count"]["value"]
                bucket["key"] /= 1000

    return new_result


def get_doc_aggs(corpus, doc_id, field):
    s = Search(index=corpus + "_terms", doc_type="term")
    s = s.query(Q("term", doc_id=doc_id))
    split = field.split(".")
    if corpusconf.is_object(split):
        raise NotImplementedError("aggs on \"" + field + "\" not implemented yet")
    if len(split) > 1:
        es_field = ".attrs.".join(split)
        is_start_q = "term.attrs." + split[0] + ".is_start"
        s = s.query(Q("term", **{is_start_q: True}))
    else:
        es_field = field
    s.aggs.bucket(field, "terms", field="term.attrs." + es_field, size=ALL_BUCKETS)
    s = s[0:0]
    result = s.execute()
    return {"aggregations": result.to_dict()["aggregations"]}


def corpus_id_to_alias(corpus):
    return corpus.split("_")[0]
