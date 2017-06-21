# -*- coding: utf-8 -*-
import codecs
import json
import logging
import os

import markdown
from flask import Flask, request
from strix.api.flask_util import crossdomain, jsonify_response
import strix.api.elasticapi as elasticapi
from strix.config import config
app = Flask(__name__)

_logger = logging.getLogger("strix.api.web")


def get_includes_excludes(request_obj):
    if request.args.get("include"):
        request_obj["includes"] = request.args.get("include").split(",")
    if request.args.get("exclude"):
        request_obj["excludes"] = request.args.get("exclude").split(",")


def get_token_lookup_sizes(request_obj):
    token_lookup_from = request.args.get("token_lookup_from")
    token_lookup_to = request.args.get("token_lookup_to")
    if token_lookup_from:
        request_obj["token_lookup_from"] = int(token_lookup_from)
    if token_lookup_to:
        request_obj["token_lookup_to"] = int(token_lookup_to)


def get_material_selection(request_obj):
    if "corpora" in request.args:
        request_obj["corpora"] = request.args.get("corpora").split(",")
    else:
        request_obj["corpora"] = elasticapi.get_all_corpora_ids()

    if "text_filter" in request.args:
        request_obj["text_filter"] = json.loads(request.args.get("text_filter"))


def get_search(request_obj):
    if "text_query" in request.args:
        # TODO remove lowercase-filter in mappingutil, then remove this
        request_obj["text_query"] = request.args.get("text_query").lower()

    if "text_query_field" in request.args:
        request_obj["text_query_field"] = request.args.get("text_query_field").replace(".", "_")


@app.route("/document/<corpus>/sentence/<sentence_id>")
@app.route("/document/<corpus>/<doc_id>")
@crossdomain(origin='*')
@jsonify_response
def get_document(corpus, doc_id=None, sentence_id=None):
    kwargs = {}
    get_includes_excludes(kwargs)
    get_token_lookup_sizes(kwargs)

    kwargs["doc_id"] = doc_id
    kwargs["sentence_id"] = sentence_id
    return elasticapi.get_document_by_id(corpus, "text", **kwargs)


@app.route("/search")
@crossdomain(origin='*')
@jsonify_response
def search():
    kwargs = {}
    get_includes_excludes(kwargs)
    get_token_lookup_sizes(kwargs)

    get_material_selection(kwargs)

    get_search(kwargs)

    if request.args.get("from"):
        kwargs["from_hit"] = int(request.args.get("from"))
    if request.args.get("to"):
        kwargs["to_hit"] = int(request.args.get("to"))

    use_highlight = True
    if request.args.get("highlight") and request.args.get("highlight") == "false":
        use_highlight = False

    if request.args.get("simple_highlight") and request.args.get("simple_highlight") == "true":
        use_highlight = False
        kwargs["simple_highlight"] = True

    if use_highlight:
        if request.args.get("highlight_number_of_fragments"):
            number_of_fragments = int(request.args.get("highlight_number_of_fragments"))
        else:
            number_of_fragments = 5
        kwargs["highlight"] = {"number_of_fragments": number_of_fragments}

    return elasticapi.search("text", **kwargs)


@app.route("/search/<corpus>/<doc_id>")
@crossdomain(origin="*")
@jsonify_response
def search_in_document(corpus, doc_id):
    kwargs = {}

    get_includes_excludes(kwargs)
    get_token_lookup_sizes(kwargs)

    if request.args.get("size"):
        kwargs["size"] = int(request.args.get("size"))

    if request.args.get("current_position"):
        kwargs["current_position"] = int(request.args.get("current_position"))

    if request.args.get("forward"):
        kwargs["forward"] = request.args.get('forward').lower() == 'true'

    get_search(kwargs)

    return elasticapi.search_in_document(corpus, "text", doc_id, **kwargs)


@app.route("/related/<corpus>/<doc_id>")
@crossdomain(origin='*')
@jsonify_response
def get_related_documents(corpus, doc_id):
    kwargs = {}

    if request.args.get("search_corpora"):
        kwargs["search_corpora"] = request.args.get("search_corpora")

    if request.args.get("relevance_function"):
        # possible_values: "more_like_this", "disjunctive_query"
        kwargs["relevance_function"] = request.args.get("relevance_function")

    # only applicable for more_like_this
    if request.args.get("min_term_freq"):
        kwargs["min_term_freq"] = request.args.get("min_term_freq")

    # only applicable for more_like_this
    if request.args.get("max_query_terms"):
        kwargs["max_query_terms"] = request.args.get("max_query_terms")

    get_includes_excludes(kwargs)
    get_token_lookup_sizes(kwargs)
    if request.args.get("from"):
        kwargs["from_hit"] = int(request.args.get("from"))
    if request.args.get("to"):
        kwargs["to_hit"] = int(request.args.get("to"))
    return elasticapi.get_related_documents(corpus, "text", doc_id, **kwargs)


@app.route("/lemgramify/<terms>")
@crossdomain(origin='*')
@jsonify_response
def autocomplete(terms):
    lemgrams = []
    for term in terms.split(","):
        lemgrams.extend(elasticapi.lemgrammify(term))
    return lemgrams


# TODO this should support the same filtering as any other call
@app.route("/field_values/<corpus>/<field>")
@crossdomain(origin="*")
@jsonify_response
def get_values(corpus, field):
    return elasticapi.get_values(corpus, "text", field)


# TODO this should support the same filtering as any other call?
@app.route("/date_histogram/<corpus>/<field>")
@crossdomain(origin="*")
@jsonify_response
def date_histogram(corpus, field):
    return elasticapi.date_histogram(corpus, "text", field, request.args)


@app.route("/aggs")
@crossdomain(origin="*")
@jsonify_response
def aggs():
    kwargs = {}
    if "facet_count" in request.args:
        kwargs["facet_count"] = int(request.args["facet_count"])
    if "exclude_empty_buckets" in request.args:
        kwargs["min_doc_count"] = 1
    if "include_facets" in request.args:
        kwargs["include_facets"] = request.args["include_facets"].split(",")
    get_material_selection(kwargs)
    res = elasticapi.get_aggs(**kwargs)
    return res


@app.route("/config")
@crossdomain(origin="*")
@jsonify_response
def get_config():
    return elasticapi.get_config()


@app.route("/")
@crossdomain(origin="*")
def get_documentation():
    input_file = codecs.open(os.path.join(config.base_dir, "resources/docs/api.md"), mode="r", encoding="utf-8")
    text = input_file.read()
    css = '<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">'
    return css + '<div style="margin-left: 20px; max-width: 750px">' + markdown.markdown(text) + '</div>'


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', threaded=True)
