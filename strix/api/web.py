# -*- coding: utf-8 -*-
import codecs
import json

import markdown
from elasticsearch_dsl.connections import connections
from flask import Flask, request
from strix.api.flask_util import crossdomain, jsonify_response
import strix.api.elasticapi as elasticapi
import strix.config as config
app = Flask(__name__)

connections.create_connection(hosts=config.elastic_hosts, timeout=120)


def get_includes_excludes():
    includes = []
    excludes = []
    if request.args.get("include"):
        includes = request.args.get("include").split(",")
    if request.args.get("exclude"):
        excludes = request.args.get("exclude").split(",")
    return includes, excludes


@app.route("/document/<corpus>/<doc_id>")
@crossdomain(origin='*')
@jsonify_response
def get_document(corpus, doc_id):
    includes, excludes = get_includes_excludes()
    return elasticapi.get_document_by_id(corpus, "text", doc_id, includes, excludes)


@app.route("/document/<corpus>/<from_hit>/<to_hit>")
@crossdomain(origin='*')
@jsonify_response
def get_documents(corpus, from_hit, to_hit):
    includes, excludes = get_includes_excludes()
    return elasticapi.get_documents(corpus, "text", int(from_hit), int(to_hit), includes, excludes)


@app.route("/search/<corpus>/<search_term>")
@app.route("/search/<corpus>/<field>/<search_term>")
@crossdomain(origin='*')
@jsonify_response
def search(corpus, search_term, field=None):
    includes, excludes = get_includes_excludes()

    kwargs = {
        "search_term": search_term
    }

    if request.args.get("from"):
        kwargs["from_hit"] = int(request.args.get("from"))
    if request.args.get("to"):
        kwargs["to_hit"] = int(request.args.get("to"))

    use_highlight = True
    if request.args.get("highlight") and request.args.get("highlight") == "false":
        use_highlight = False

    if use_highlight:
        if request.args.get("highlight_number_of_fragments"):
            number_of_fragments = int(request.args.get("highlight_number_of_fragments"))
        else:
            number_of_fragments = 5
        kwargs["highlight"] = {"number_of_fragments": number_of_fragments}

    if includes:
        kwargs["includes"] = includes
    if excludes:
        kwargs["excludes"] = excludes
    kwargs["field"] = field

    return elasticapi.search(corpus, "text", **kwargs)


@app.route("/search/<corpus>/<doc_id>/<field>/<value>")
@crossdomain(origin="*")
@jsonify_response
def search_in_document(corpus, doc_id, field, value):
    kwargs = {}

    if request.args.get("size"):
        kwargs["size"] = int(request.args.get("size"))

    if request.args.get("current_position"):
        kwargs["current_position"] = int(request.args.get("current_position"))

    if request.args.get("forward"):
        kwargs["forward"] = request.args.get('forward').lower() == 'true'

    # TODO remove lowercase-filter in mappingutil, then remove this
    value = value.lower()

    return elasticapi.search_in_document(corpus, "text", doc_id, field, value, **kwargs)



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
            result[corpus] = json.load(open("resources/config/" + corpus + ".json"))["analyze_config"]
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
    input_file = codecs.open("resources/docs/api.md", mode="r", encoding="utf-8")
    text = input_file.read()
    css = '<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">'
    return css + '<div style="margin-left: 20px; width: 750px">' + markdown.markdown(text) + '</div>'

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', threaded=True)
