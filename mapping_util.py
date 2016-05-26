from elasticsearch_dsl import analysis, analyzer, tokenizer
import os

def annotation_analyzer(annotation_name, isSet=False):
    """
    create an analyzer for a specific annotation found a token s.a. framtid|wid=12|page=3||
    for example passing "wid" as parameter and using anayzer on a field in a type "text "will enable
    searching on text.wid
    @param isSet: if true, the pattern annotation_name=value gives value as a comma-separated list. 
    """
    filter_name = annotation_name + "_filter"
    analyzer_name = annotation_name + "_analyzer"
    patterns = []
    """
    so this doesn't work. i can't make a regexp that can find 
    Korp's set attributes (such as lemgram) and yield a capturing group for 
    each member of the set :( 
    """
    if isSet:
        pass
        # patterns.append("\\|" + annotation_name + "=(?:(.+?)_)+\\|")
        # patterns.append("\\|" + annotation_name + "=(.*?)\\|")
        # patterns.append("\\|" + annotation_name + "=(.*?)_(.*?)\\|")
        # patterns.append("\\|" + annotation_name + "=(.+?)_?(.+?)?_?(.+?)?\\|")
    else:
        pass
    # because set isn't supported, assume one value only in annotation
    patterns.append("\\|" + annotation_name + "=(.+?)\\|")



    a_filter = analysis.token_filter(filter_name, 'pattern_capture', preserve_original=False, patterns=patterns)
    return analysis.analyzer(analyzer_name, tokenizer='whitespace', filter=["lowercase", a_filter])


def get_swedish_analyzer():
    """
    uses pattern_capture token filter to change input from framtid|wid=12|page=3|| to token "framtid"
    """
    stems_file = os.getcwd()+ "/analyzers/stems.txt"
    payload_strip = analysis.token_filter('payload_strip', 'pattern_capture', preserve_original=False, patterns=["^(.*?)\\|.*"])
    stemmer = analysis.token_filter("swedish_stemmer", type="stemmer_override", rules_path=stems_file)
    return analyzer('swedish',  tokenizer=tokenizer('whitespace'), filter=['lowercase', payload_strip, stemmer])


def default_annotation_analyzer():
    """
    uses pattern_capture token filter to change input framtid|wid=12|page=3|| into three tokens -> framtid, wid=12 and page=3
    """
    a_filter = analysis.token_filter("payload_splitter", 'pattern_capture', preserve_original=False, patterns=["(.+?)\\|"])
    return analysis.analyzer("annotations_analyzer", tokenizer='whitespace', filter=["lowercase", a_filter])
