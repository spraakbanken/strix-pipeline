# -*- coding: utf-8 -*-
from elasticsearch_dsl import analysis, analyzer

token_separator = "\u241D"
annotation_separator = "\u241E"
set_delimiter = "\u241F"
empty_set = "\u2205"


def annotation_analyzer(annotation_name, is_set=False):
    """
    create an analyzer for a specific annotation found a token s.a. framtid|wid=12|page=3||
    for example passing "wid" as parameter and using anayzer on a field in a type "text "will enable
    searching on text.wid
    :param is_set:
    :param annotation_name:
    """
    filter_name = annotation_name + "_filter"
    analyzer_name = annotation_name + "_analyzer"

    annotation_filter = analysis.token_filter(filter_name, "pattern_capture", preserve_original=False, patterns=[
        annotation_separator + annotation_name + "=(.*?)" + annotation_separator
    ])
    token_filters = ["lowercase", annotation_filter]

    if is_set:
        set_filter = analysis.token_filter("set_token_filter", "set_delimiter_token_filter", delimiter=set_delimiter)
        token_filters.append(set_filter)
    else:
        stop_empty_filter = analysis.token_filter("stop", "stop", stopwords=[empty_set])
        token_filters.append(stop_empty_filter)

    return analysis.analyzer(analyzer_name, tokenizer=pattern_tokenizer(), filter=token_filters)


def get_standard_analyzer():
    return analyzer("standard", tokenizer="standard", filter=["lowercase"])


def get_token_annotation_analyzer():
    """
    uses pattern_capture token filter to change input from FrAmTiD|wid=12|page=3|| to token "framtid"
    """
    payload_strip = analysis.token_filter("payload_strip", "pattern_capture", preserve_original=False, patterns=[
        "^(.*?)" + annotation_separator + ".*"
    ])
    return analyzer("word",  tokenizer=pattern_tokenizer(), filter=["lowercase", payload_strip])


def as_you_type_analyzer():
    as_you_type_filter = analysis.token_filter("as_you_type_filter", "edge_ngram", min_gram=1, max_gram=20)
    return analysis.analyzer("as_you_type_analyzer", tokenizer="standard", filter=["lowercase", as_you_type_filter])


def pattern_tokenizer():
    return analysis.tokenizer("strix_tokenizer", "pattern", pattern=token_separator)


def get_swedish_analyzer():
    stemmer = analysis.token_filter("swedish_stemmer", type="stemmer_override", rules_path="stems.txt")
    return analyzer("swedish",  tokenizer="standard", filter=["lowercase", stemmer])


def similarity_tags_analyzer():
    return analysis.analyzer("similarity_tags", tokenizer="whitespace", filter=["lowercase"])


def ranked_text_analyzer(annotation_name):
    rank_strip = analysis.token_filter("rank_strip", "pattern_capture", preserve_original=False, patterns=["^(.*?):.*"])
    return analysis.analyzer(annotation_name + "_text_analyzer", tokenizer="keyword", filter=[rank_strip])
