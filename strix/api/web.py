# -*- coding: utf-8 -*-
import codecs
import json
import logging
import os

import markdown
from elasticsearch_dsl.connections import connections
from flask import Flask, request
from strix.api.flask_util import crossdomain, jsonify_response
import strix.api.elasticapi as elasticapi
from strix.config import config
import strix.loghelper
app = Flask(__name__)

connections.create_connection(hosts=config.elastic_hosts, timeout=120)
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
        token_lookup_from = int(token_lookup_from)
    if token_lookup_to:
        token_lookup_to = int(token_lookup_to)
    request_obj["token_lookup_from"] = token_lookup_from
    request_obj["token_lookup_to"] = token_lookup_to


@app.route("/document/<corpus>/<doc_id>")
@crossdomain(origin='*')
@jsonify_response
def get_document(corpus, doc_id):
    kwargs = {}
    get_includes_excludes(kwargs)
    get_token_lookup_sizes(kwargs)
    return elasticapi.get_document_by_id(corpus, "text", doc_id, **kwargs)


@app.route("/search/<corpus>")
@app.route("/search/<corpus>/")
@app.route("/search/<corpus>/<search_term>")
@app.route("/search/<corpus>/<field>/<search_term>")
@crossdomain(origin='*')
@jsonify_response
def search(corpus, search_term=None, field=None):
    kwargs = {}
    get_includes_excludes(kwargs)
    get_token_lookup_sizes(kwargs)

    if "text_filter" in request.args:
        kwargs["text_filter"] = json.loads(request.args.get("text_filter"))

    if search_term:
        kwargs["search_term"] = search_term

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

    if field:
        kwargs["field"] = field.replace(".", "_")

    return elasticapi.search(corpus, "text", **kwargs)


@app.route("/search/<corpus>/doc_id/<doc_id>/<search_term>")
@app.route("/search/<corpus>/doc_id/<doc_id>/<field>/<search_term>")
@crossdomain(origin="*")
@jsonify_response
def search_in_document(corpus, doc_id, search_term, field=None):
    kwargs = {}

    get_includes_excludes(kwargs)
    get_token_lookup_sizes(kwargs)

    if request.args.get("size"):
        kwargs["size"] = int(request.args.get("size"))

    if request.args.get("current_position"):
        kwargs["current_position"] = int(request.args.get("current_position"))

    if request.args.get("forward"):
        kwargs["forward"] = request.args.get('forward').lower() == 'true'

    if field and "." in field:
        field = field.replace(".", "_")

    kwargs["field"] = field

    # TODO remove lowercase-filter in mappingutil, then remove this
    value = search_term.lower()

    return elasticapi.search_in_document(corpus, "text", doc_id, value, **kwargs)


@app.route("/related/<corpus>/<doc_type>/<doc_id>")
@crossdomain(origin='*')
@jsonify_response
def get_related_documents(corpus, doc_type, doc_id):
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
    return elasticapi.get_related_documents(corpus, doc_type, doc_id, **kwargs)


@app.route("/lemgramify/<terms>")
@crossdomain(origin='*')
@jsonify_response
def autocomplete(terms):
    lemgrams = []
    for term in terms.split(","):
        lemgrams.extend(elasticapi.lemgrammify(term))
    return lemgrams


@app.route("/field_values/<corpus>/<field>")
@crossdomain(origin="*")
@jsonify_response
def get_values(corpus, field):
    return elasticapi.get_values(corpus, "text", field)


@app.route("/config")
@app.route("/config/<corpora>")
@crossdomain(origin="*")
@jsonify_response
def get_config(corpora=None):
    if corpora:
        result = {}
        for corpus in corpora.split(","):
            result[corpus] = json.load(open(os.path.join(config.base_dir, "resources/config/" + corpus + ".json")))["analyze_config"]
        return result
    else:
        result = elasticapi.es.cat.indices(h="index")
        indices = []
        for index in result.split("\n"):
            if not (index == ".kibana" or index.endswith("_search") or index.endswith("_terms") or index == "" or index == "litteraturbanken"):
                indices.append(index)
        return indices


@app.route("/")
@crossdomain(origin="*")
def get_documentation():
    input_file = codecs.open(os.path.join(config.base_dir, "resources/docs/api.md"), mode="r", encoding="utf-8")
    text = input_file.read()
    css = '<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">'
    return css + '<div style="margin-left: 20px; max-width: 750px">' + markdown.markdown(text) + '</div>'


if __name__ == "__main__":
    strix.loghelper.setup_console_logging()
    app.run(debug=True, host='0.0.0.0', threaded=True)
