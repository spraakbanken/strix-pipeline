from elasticsearch_dsl import Search, Q


def highlight_search(hit_corpus, hit, item, highlight=None, simple_highlight=None):
    if highlight or simple_highlight:
        highlights = process_hit(hit_corpus, hit, 5, include_annotations=not simple_highlight)
        if simple_highlight:
            item["highlight"] = get_simple_kwic(highlights)
        else:
            item["highlight"] = highlights


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


def process_hit(corpus, hit, context_size, include_annotations=True):
    """
    takes a hit and extracts positions from highlighting and extracts
    tokens + attributes
    :param corpus: the corpus of the hit
    :param hit: a non-parsed hit that has used the strix-highlighting
    :param context_size: how many tokens should be shown to each side of the match
    :param include_annotations: if all annotations should be returned or just word and whitespace
    :return: hit-element with added highlighting
    """
    doc_id = hit["doc_id"]
    doc_type = hit.meta.doc_type

    if hasattr(hit.meta, "highlight"):
        highlights = get_kwic(corpus, doc_id, doc_type, hit.meta.highlight.positions, context_size, include_annotations=include_annotations)
    else:
        highlights = []

    return {
        "highlight": highlights,
        "total_doc_highlights": len(highlights),
        "doc_id": doc_id
    }


def get_simple_kwic(highlights):
    result = []

    def get_whitespace(token):
        return token.get("whitespace", "").replace("\n", " ")

    def get_token(token, sep=""):
        return token["word"] + sep + get_whitespace(token)

    def stringify(highlight_part):
        return "".join([get_token(token) for token in highlight_part])

    for highlight in highlights["highlight"]:
        left = stringify(highlight["left_context"])
        match_start = "<em>" + stringify(highlight["match"][0:len(highlight["match"]) - 1])
        match_end = get_token(highlight["match"][-1], sep="</em>")
        right = stringify(highlight["right_context"])
        result.append(left + match_start + match_end + right.rstrip())
    highlights["highlight"] = result
    return highlights


def get_kwic(corpus, doc_id, doc_type, spans, context_size, include_annotations=True):
    spans = get_spans(spans)
    term_index = get_term_index_for_doc(corpus, doc_id, doc_type, spans, context_size, include_annotations=include_annotations)
    if not term_index:
        raise RuntimeError("It was not possible to fetch term index for corpus: " + corpus + ", doc_id: " + doc_id + ", doc_type: " + doc_type)

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
    positions = set()
    for (from_int, to_int) in spans:
        positions.update(set(range(from_int, to_int)))

        if context_size > 0:
            positions.update(set(range(from_int - context_size, from_int)))
            positions.update(set(range(to_int, to_int + context_size)))

    return get_terms_for_doc(corpus, doc_type, doc_id, positions=list(positions), include_annotations=include_annotations)


def get_terms_for_doc(corpus, doc_type, doc_id, positions=(), from_pos=None, size=None, include_annotations=True):
    """

    :param corpus:
    :param doc_type:
    :param doc_id:
    :param positions:
    :param from_pos:
    :param size:
    :param include_annotations:
    :return:
    """
    term_index = {}

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

    query = Q("constant_score", filter=Q("bool", must=must_clauses))

    s = Search(index=corpus + "_terms", doc_type="term").query(query)
    s.sort("_doc")

    if not include_annotations:
        s = s.source(includes=("position", "term.word", "term.whitespace"))

    for hit in s.scan():
        source = hit.to_dict()
        term_index[source["position"]] = source["term"]

    return term_index


def get_spans(string_spans):
    spans = []
    for span in string_spans:
        [_from, _to] = span.split("-")
        spans.append((int(_from), int(_to)))
    return spans
