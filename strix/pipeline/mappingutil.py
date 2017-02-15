# -*- coding: utf-8 -*-
from elasticsearch_dsl import analysis, analyzer
from strix.config import config


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

    annotation_filter = analysis.token_filter(filter_name, "pattern_capture", preserve_original=False, patterns=["\u241E" + annotation_name + "=(.+?)\u241E"])
    token_filters = ["lowercase", annotation_filter]

    if is_set:
        set_filter = analysis.token_filter("set_token_filter", "set_delimiter_token_filter", delimiter="\u241F")
        token_filters.append(set_filter)

    return analysis.analyzer(analyzer_name, tokenizer=pattern_tokenizer(), filter=token_filters)


def get_standard_analyzer():
    """
    uses pattern_capture token filter to change input from FrAmTiD|wid=12|page=3|| to token "framtid"
    """
    payload_strip = analysis.token_filter("payload_strip", "pattern_capture", preserve_original=False, patterns=["^(.*?)\u241E.*"])
    return analyzer("word",  tokenizer=pattern_tokenizer(), filter=["lowercase", payload_strip])


def as_you_type_analyzer():
    as_you_type_filter = analysis.token_filter("as_you_type_filter", "edge_ngram", min_gram=1, max_gram=20)
    return analysis.analyzer("as_you_type_analyzer", tokenizer="standard", filter=["lowercase", as_you_type_filter])


def pattern_tokenizer():
    return analysis.tokenizer("strix_tokenizer", "pattern", pattern="\u241D")


def get_swedish_analyzer():
    stems_file = config.stems_file
    stemmer = analysis.token_filter("swedish_stemmer", type="stemmer_override", rules_path=stems_file)
    return analyzer("swedish",  tokenizer="standard", filter=["lowercase", stemmer])