from elasticsearch_dsl import analysis, analyzer, tokenizer


def annotation_analyzer(annotation_name):
    """
    create an analyzer for a specific annotation found a token s.a. framtid|wid=12|page=3||
    for example passing "wid" as parameter and using anayzer on a field in a type "text "will enable
    searching on text.wid
    """
    filter_name = annotation_name + "_filter"
    analyzer_name = annotation_name + "_analyzer"

    a_filter = analysis.token_filter(filter_name, 'pattern_capture', preserve_original=False, patterns=["\\|"
                                     + annotation_name + "=(.+?)\\|"])
    return analysis.analyzer(analyzer_name, tokenizer='whitespace', filter=["lowercase", a_filter])


def get_swedish_analyzer():
    """
    uses pattern_capture token filter to change input from framtid|wid=12|page=3|| to token "framtid"
    """
    payload_strip = analysis.token_filter('payload_strip', 'pattern_capture', preserve_original=False, patterns=["^(.*?)\\|.*"])
    return analyzer('swedish',  tokenizer=tokenizer('whitespace'), filter=['lowercase', payload_strip])


def default_annotation_analyzer():
    """
    uses pattern_capture token filter to change input framtid|wid=12|page=3|| into three tokens -> framtid, wid=12 and page=3
    """
    a_filter = analysis.token_filter("payload_splitter", 'pattern_capture', preserve_original=False, patterns=["(.+?)\\|"])
    return analysis.analyzer("annotations_analyzer", tokenizer='whitespace', filter=["lowercase", a_filter])
