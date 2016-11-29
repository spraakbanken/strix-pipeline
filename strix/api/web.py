# -*- coding: utf-8 -*-
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


@app.route("/document/<corpus>/<doc_type>/<doc_id>")
@crossdomain(origin='*')
@jsonify_response
def get_document(corpus, doc_type, doc_id):
    includes, excludes = get_includes_excludes()
    return elasticapi.get_document_by_id(corpus, doc_type, doc_id, includes, excludes)


@app.route("/document/<corpus>/<doc_type>/<from_page>/<to_page>")
@crossdomain(origin='*')
@jsonify_response
def get_documents(corpus, doc_type, from_page, to_page):
    includes, excludes = get_includes_excludes()
    return elasticapi.get_documents(corpus, doc_type, int(from_page), int(to_page), includes, excludes)


@app.route("/search/<corpus>/<doc_type>/<search_term>")
@crossdomain(origin='*')
@jsonify_response
def search(corpus, doc_type, search_term):
    includes, excludes = get_includes_excludes()

    kwargs = {
        "search_term": search_term
    }

    if request.args.get("from"):
        kwargs["from_hit"] = int(request.args.get("from"))
    if request.args.get("to"):
        kwargs["to_hit"] = int(request.args.get("to"))

    use_highlight = True
    if request.args.get("highlight") and not bool(request.args.get("highlight")):
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

    return elasticapi.search(corpus, doc_type, **kwargs)


@app.route("/lemgramify/<terms>")
@crossdomain(origin='*')
@jsonify_response
def autocomplete(terms):
    lemgrams = []
    for term in terms.split(","):
        lemgrams.extend(elasticapi.lemgrammify(term))
    return lemgrams

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', threaded=True)