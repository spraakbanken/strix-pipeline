# -*- coding: utf-8 -*-
from elasticsearch_dsl import analysis, analyzer

token_separator = "\u241D"
set_delimiter = "\u241F"
empty_set = "\u2205"


def set_annotation_analyzer():
    set_filter = analysis.token_filter("set_token_filter", "set_delimiter_token_filter", delimiter=set_delimiter)
    token_filters = ["lowercase", set_filter]
    return analysis.analyzer("set_annotation_analyzer", tokenizer=pattern_tokenizer(), filter=token_filters)


def annotation_analyzer():
    stop_empty_filter = analysis.token_filter("stop", "stop", stopwords=[empty_set])
    token_filters = ["lowercase", stop_empty_filter]
    return analysis.analyzer("annotation_analyzer", tokenizer=pattern_tokenizer(), filter=token_filters)


def get_standard_analyzer():
    return analyzer("standard", tokenizer="standard", filter=["lowercase"])


def token_analyzer():
    return analyzer("word",  tokenizer=pattern_tokenizer(), filter=["lowercase"])


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
