# -*- coding: utf-8 -*-
from flask import Flask, request
from strix.flask_util import crossdomain, jsonify_response
import strix.elasticapi as elasticapi
app = Flask(__name__)


def get_includes_excludes():
    includes = []
    excludes = []
    if request.args.get("include"):
        includes = request.args.get("include").split(",")
    if request.args.get("exclude"):
        excludes = request.args.get("exclude").split(",")
    return includes, excludes


@app.route("/")
def info():
    return "TODO send to documentation?"


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


@app.route("/search/<corpus>/<doc_type>/<search_field>/<search_term>")
@crossdomain(origin='*')
@jsonify_response
def search(corpus, doc_type, search_field, search_term):
    includes, excludes = get_includes_excludes()
    return elasticapi.search(corpus, doc_type, search_field, search_term, includes, excludes)



