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
from strix.api.elasticapihelpers import page_size
app = Flask(__name__)

_logger = logging.getLogger("strix.api.web")


def get_includes_excludes(request_obj):
    if request.args.get("include"):
        request_obj["includes"] = request.args.get("include").split(",")
    if request.args.get("exclude"):
        request_obj["excludes"] = request.args.get("exclude").split(",")


def get_token_lookup_sizes(request_obj):
    try:
        token_lookup_from = request.args.get("token_lookup_from")
        if token_lookup_from:
            token_lookup_from = int(token_lookup_from)
            token_lookup_to = request.args.get("token_lookup_to")
            if token_lookup_to:
                token_lookup_to = int(token_lookup_to)
            else:
                token_lookup_to = None
            request_obj["token_lookup_size"] = page_size(token_lookup_from, token_lookup_to, limit=False)
    except ValueError:
        raise ValueError("token lookup sizes must be integers")


def get_material_selection(request_obj):
    if "corpora" in request.args:
        request_obj["corpora"] = request.args.get("corpora").split(",")
    else:
        request_obj["corpora"] = elasticapi.get_all_corpora_ids()

    if "text_filter" in request.args:
        request_obj["text_filter"] = json.loads(request.args.get("text_filter"))


def get_search(request_obj):
    can_use_highlight = False
    if "text_query" in request.args:
        # TODO remove lowercase-filter in mappingutil, then remove this
        request_obj["text_query"] = request.args.get("text_query").lower()
        can_use_highlight = True

    if "text_query_field" in request.args:
        request_obj["text_query_field"] = request.args.get("text_query_field").replace(".", "_")

    return can_use_highlight


def get_result_size(request_obj):
    try:
        from_param = request.args.get("from")
        if from_param:
            kwargs = {"from_hit": int(from_param)}
            to_param = request.args.get("to")
            if to_param:
                kwargs["to_hit"] = int(to_param)
            request_obj["size"] = page_size(**kwargs)
    except ValueError:
        raise ValueError("from / to must be integers")


@app.route("/document/<corpus>/sentence/<sentence_id>")
@app.route("/document/<corpus>/<doc_id>")
@crossdomain(origin="*")
@jsonify_response
def get_document(corpus, doc_id=None, sentence_id=None):
    kwargs = {}
    get_includes_excludes(kwargs)
    get_token_lookup_sizes(kwargs)

    kwargs["doc_id"] = doc_id
    kwargs["sentence_id"] = sentence_id
    return elasticapi.get_document_by_id(corpus, "text", **kwargs)


@app.route("/search")
@crossdomain(origin="*")
@jsonify_response
def search():
    kwargs = {}
    get_includes_excludes(kwargs)
    get_token_lookup_sizes(kwargs)

    get_material_selection(kwargs)

    use_highlight = get_search(kwargs)

    get_result_size(kwargs)

    if request.args.get("simple_highlight") and request.args.get("simple_highlight") == "true":
        if use_highlight:
            use_highlight = False
            kwargs["simple_highlight"] = True
    elif request.args.get("highlight") and request.args.get("highlight") == "false":
        use_highlight = False

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
        kwargs["forward"] = request.args.get("forward").lower() == "true"

    get_search(kwargs)

    return elasticapi.search_in_document(corpus, "text", doc_id, **kwargs)


@app.route("/related/<corpus>/<doc_id>")
@crossdomain(origin="*")
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
    get_result_size(kwargs)
    return elasticapi.get_related_documents(corpus, "text", doc_id, **kwargs)


@app.route("/lemgramify/<terms>")
@crossdomain(origin="*")
@jsonify_response
def autocomplete(terms):
    return elasticapi.lemgrammify_many(terms.split(","))


# TODO this should support the same filtering as any other call?
# DEPRACATED use aggs?corpora=corpus&include_facets=field
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


@app.route("/aggs/<corpus>/<doc_id>/<field>")
@crossdomain(origin="*")
@jsonify_response
def document_aggs(corpus, doc_id, field):
    return elasticapi.get_doc_aggs(corpus, doc_id, field)


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
    app.run(debug=True, host="0.0.0.0", threaded=True)
