from elasticsearch_dsl import Search, Q


def highlight_search(documents, hits, highlight=None, simple_highlight=None, corpus_id=None):
    context_size = 5
    if highlight or simple_highlight:
        term_index = get_term_index(documents, context_size, include_annotations=not simple_highlight)
        for result in hits:
            item = result["item"]
            doc_id = item["doc_id"]
            doc_type = item["doc_type"]
            corpus_id = corpus_id or item["corpus_id"]

            doc_term_index = term_index.get(corpus_id, {}).get(doc_type, {}).get(doc_id, {})
            if doc_term_index:
                positions = result["positions"]
                if positions != "preview":
                    highlights = get_kwic(positions, context_size, doc_term_index)
                else:
                    highlights = []
                    item["preview"] = get_simple_kwic(get_preview(doc_term_index))[0]
            else:
                highlights = []

            if simple_highlight:
                highlights = get_simple_kwic(highlights)
            else:
                highlights = highlights

            item["highlight"] = {
                "highlight": highlights,
                "total_doc_highlights": len(highlights),
                "doc_id": doc_id
            }


def get_document_highlights(corpus, es_id, doc_type, spans):
    spans = get_spans(spans)
    term_index = get_term_index_for_doc(corpus, es_id, doc_type, spans, 0)

    all_highlights = []
    for i, (from_int, to_int) in enumerate(spans):
        highlights = []
        for pos in range(from_int, to_int):
            highlights.append(term_index[pos]["attrs"])

        all_highlights.append({"order": i, "highlights": highlights})
    return all_highlights


def add_highlight_to_doc(corpus, doc_type, doc_id, hit, current_position=-1, size=None, forward=True):
    if size != 0 and hasattr(hit.meta, "highlight"):
        count = 0
        positions = hit.meta.highlight.positions
        if not forward:
            positions.reverse()

        get_positions = []
        for span_pos in positions:
            pos = int(span_pos.split("-")[0])
            if forward and pos > current_position or not forward and pos < current_position:
                get_positions.append(pos)
                count += 1

            if size and size <= count:
                break
        terms = get_terms_for_doc(corpus, doc_type, doc_id, positions=get_positions)
        highlight = list(terms.values())

        if not forward:
            highlight.reverse()
    else:
        highlight = []
    return highlight


def get_simple_kwic(highlights):
    result = []

    def get_whitespace(token):
        return token.get("whitespace", "").replace("\n", " ")

    def get_token(token, sep=""):
        return token["word"] + sep + get_whitespace(token)

    def stringify(highlight_part):
        return "".join([get_token(token) for token in highlight_part])

    for highlight in highlights:
        left = stringify(highlight["left_context"])
        if "match" in highlight:
            match_start = "<em>" + stringify(highlight["match"][0:len(highlight["match"]) - 1])
            match_end = get_token(highlight["match"][-1], sep="</em>")
        else:
            match_start = ""
            match_end = ""
        if "right_context" in highlight:
            right = stringify(highlight["right_context"])
        else:
            right = ""
        result.append(left + match_start + match_end + right.rstrip())
    return result


def get_kwic(spans, context_size, term_index):
    highlights = []

    for (from_int, to_int) in spans:
        left = []
        match = []
        right = []

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


def get_preview(term_index):
    preview_tokens = []
    for (_, value) in sorted(term_index.items()):
        preview_tokens.append(value)
    return [{"left_context": preview_tokens}]


def get_term_index_for_doc(corpus, doc_id, doc_type, spans, context_size, include_annotations=True):
    """
    Gets terms for a document
    :param corpus:
    :param doc_id:
    :param doc_type:
    :param spans: strings formatted as "3-6", get all terms from position 3 to 6
    :param context_size: how many terms should be included before and after each span
    :param include_annotations: if all annotations should be returned or just word and whitespace
    :return: a dictionary of position -> term of the requested document
    """
    documents = {corpus: {doc_type: {doc_id: spans}}}
    return get_term_index(documents, context_size, include_annotations=include_annotations)[corpus][doc_type][doc_id]


def get_spans_for_highlight(result_obj, documents, corpus, doc_type, doc_id, hit, include_preview=True):
    if corpus not in documents:
        documents[corpus] = {}
    if doc_type not in documents[corpus]:
        documents[corpus][doc_type] = {}
    if hasattr(hit.meta, "highlight"):
        positions = get_spans(hit.meta.highlight.positions)
        documents[corpus][doc_type][doc_id] = positions
        result_obj["positions"] = positions
    elif include_preview:
        preview_pos = [(0, 50)]
        documents[corpus][doc_type][doc_id] = preview_pos
        result_obj["positions"] = "preview"


def get_term_index(documents, context_size, include_annotations=True):
    for corpus, doc_types in documents.items():
        for doc_type, doc_ids in doc_types.items():
            for doc_id, spans in doc_ids.items():
                positions = set()
                for (from_int, to_int) in spans:
                    positions.update(set(range(from_int, to_int)))

                    if context_size > 0:
                        positions.update(set(range(from_int - context_size, from_int)))
                        positions.update(set(range(to_int, to_int + context_size)))
                documents[corpus][doc_type][doc_id] = list(positions)

    return get_terms(documents, include_annotations=include_annotations)


def get_terms(documents, include_annotations=True):
    should_clauses = []
    for corpus, doc_types in documents.items():
        for doc_type, doc_ids in doc_types.items():
            for doc_id, positions in doc_ids.items():
                should_clauses.append(get_term_index_query(corpus, doc_type, doc_id, positions=positions))
                documents[corpus][doc_type][doc_id] = {}

    if len(should_clauses) == 0:
        return documents

    query = Q("bool", should=should_clauses)

    s = Search(index="*_terms", doc_type="term").query(query)
    s.sort("_doc")

    if not include_annotations:
        s = s.source(includes=("position", "term.word", "term.whitespace", "doc_type", "doc_id"))

    for hit in s.scan():
        source = hit.to_dict()
        corpus = hit.meta.index.split("_")[0]
        doc_type = source["doc_type"]
        doc_id = source["doc_id"]
        documents[corpus][doc_type][doc_id][source["position"]] = source["term"]

    return documents


def get_terms_for_doc(corpus, doc_type, doc_id, positions=(), from_pos=None, size=None, include_annotations=True):
    term_index = {}

    query = get_term_index_query(corpus, doc_type, doc_id, positions=positions, from_pos=from_pos, size=size)

    s = Search(index=corpus + "_terms", doc_type="term").query(query)
    s.sort("_doc")

    if not include_annotations:
        s = s.source(includes=("position", "term.word", "term.whitespace"))

    for hit in s.scan():
        source = hit.to_dict()
        term_index[source["position"]] = source["term"]

    return term_index


def get_term_index_query(corpus, doc_type, doc_id, positions=(), from_pos=None, size=None):
    must_clauses = []
    if positions:
        must_clauses.append(Q("terms", position=positions))
    elif from_pos or size:
        if not from_pos:
            from_pos = 0
        position_range = {"gte": from_pos}
        if size:
            position_range["lte"] = from_pos + size - 1
        must_clauses.append(Q("range", position=position_range))

    must_clauses.append(Q("term", doc_id=doc_id))
    must_clauses.append(Q("term", doc_type=doc_type))
    must_clauses.append(Q("term", _index=corpus + "_terms"))

    return Q("constant_score", filter=Q("bool", must=must_clauses))


def get_spans(string_spans):
    spans = []
    for span in string_spans:
        [_from, _to] = span.split("-")
        spans.append((int(_from), int(_to)))
    return spans
