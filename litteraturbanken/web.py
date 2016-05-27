# -*- coding: utf-8 -*-
import flask
from flask import Flask, request
import logging, sys, re
from elasticsearch_dsl import Search, Q
from strix.flask_util import crossdomain, jsonify_response
import strix.litteraturbanken.elasticapi as lbelasticapi
import strix.config as config
import elasticsearch

app = Flask(__name__)
es = elasticsearch.Elasticsearch(config.elastic_hosts)

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
    return "TODO send to documentation"


@app.route("/get_authors")
@crossdomain(origin='*')
@jsonify_response
def get_authors():
    includes, excludes = get_includes_excludes()
    sort_field = "name_for_index"
    return lbelasticapi.get_documents("author", 0, 10000, includes, excludes, sort_field)


@app.route("/get_lb_author/<author_id>")
@crossdomain(origin='*')
@jsonify_response
def get_author(author_id):
    includes, excludes = get_includes_excludes()
    return lbelasticapi.get_document_by_id("author", author_id, includes, excludes)

# doc_types[]: lista av 'work' eller 'title'
# authors[]: lista av authorid
# filter_string: delsträngsfiltrering på titel och author
@app.route("/lb_list_all/<doc_types>")
@app.route("/lb_list_all/<doc_types>/<authors>")
@crossdomain(origin="*")
@jsonify_response
def list_all(doc_types, authors=""):
    """
    query param: filter_string for filtering on relevant fields
    """
    includes, excludes = get_includes_excludes()

    filter_string = request.args.get("filter_string", "")
    _from = int(request.args.get("from", "0"))
    _to = int(request.args.get("to", "10"))

    if authors:
        authors_list = authors.split(",")
        return lbelasticapi.search_work_by_authors(doc_types, authors_list, _from, _to, includes, excludes)
    elif filter_string:
        return lbelasticapi.search_work_by_filter(doc_types, filter_string, _from, _to, includes, excludes)
    else:
        return lbelasticapi.get_documents(doc_types, _from, _to, includes, excludes)

@app.route("/get_work_info/<authorid>/<titlepath>")
@crossdomain(origin='*')
def getWorkInfo(authorid, titlepath, mediatype=None): # i strix: 'getDocument'
    # returnerar metadatan för hela verket, inklusive sidmappning id->namn
    body = {
       "filter": {
            "term": {
                 "title_id": titlepath
            }
        }
    }

    data = es.search(index="litteraturbanken", doc_type="faksimil,etext", body=body)["hits"]

    return flask.jsonify(data["hits"][0]["_source"])


def searchIntro():
    body = {
        "sort" : [
            { "name_for_index" : "asc" }
        ],
        "query": {

            "match_phrase": {
               "intro": "Uppsala"
           }
        },
       "highlight": {
           "fields": {
               "intro": {}
           }
       }
    }


def getOCR(lbworkid, pageix):
    pass



def logPageRequest(lbworkid, pageix, mediatyp): # kanske kan ersättas med apache-logparsning i framtiden
    pass


def getStats(): # för statistiksidan
    pass


def searchLexicon(word): # förhoppningsvis går det att lägga Svensk ordbok i Karp
    pass


@app.route("/get_epub")
@crossdomain(origin='*')
@jsonify_response
def get_epub():
    includes, excludes = get_includes_excludes()
    return lbelasticapi.get_work_by_field("epub", 0, int(request.args.get("size", 20)), includes, excludes)


@app.route("/lb_work_search/<lbworkid>/<query>")
@crossdomain(origin='*')
@jsonify_response
def search_lb_work(lbworkid):
    return Search(using=es, index="litteraturbanken") \
    .filter("term", lbworkid=lbworkid) \
    .query("match_phrase", text=query) \
    .execute()

    # .query(~Q("match", description="beta"))



# tar många olika parametrar typ proofread: false, prefix: true osv osv
@app.route("/lb_search/<query>")
@crossdomain(origin='*')
def lb_search(query):
    # TODO: implement authors search
    # authors = request.args.get("authors", "").split(",")
    # s = Search(using=es, index="litteraturbanken") \
    #     .filter("term", **{"authors.authorid" : "SvenssonC"})
    # s.query = Q('nested', path="authors", query=s.query)


    s = Search(using=es, index="litteraturbanken") \
        .extra(explain=True) \
        .query("match_phrase", **{"text": query}) \
        .highlight("text",
            type="fvh",
            fragment_size=500,
            number_of_fragments=20,
            boundary_chars="||",
            boundary_max_scan=10) \
        .source(exclude=["parts", "text", "sourcedesc" ])
    s.aggs.bucket("authors", "nested", path="authors") \
        .bucket("author_id", "terms", field="authors.author_id", size=0) \
            .bucket("title", "terms", field="lbworkid")

    result = s.execute()

    output = []

    for hit in result:
        highlights = []

        for row in hit.meta.highlight.text:

            match = r"<em>(.*?)</em>"
            left_context = re.split(match, row)[0].split("||")
            right_context = re.split(match, row)[-1].split("||")

            def formatContext(context):
                return [{"word" : word.split("|")[0].strip(), "attrs" : word.split("|")[1].split(",")}
                            for word in context
                            if "|" in word]

            left_context = list(formatContext(left_context))
            right_context = list(formatContext(right_context))
            d = {
                "left_context" : left_context,
                "match": formatContext(re.search(match, row, re.S).groups()[0].split("||")),
                "right_context" : right_context
            }
            # print(d)
            highlights.append(d)

        print(list(hit.meta.to_dict().keys()))
        # extract term frequency from explain. not sure this works very well.
        try:
            term_freq_value = hit.meta.explanation["details"][0]["details"][0]["details"][0]["details"][0]["value"]
        except KeyError:
            term_freq_value = "KEYERROR"

        output.append({
            "highlight" : highlights,
            "num_highlight" : len(highlights),
            "source" : hit.to_dict(),
            "total_highlights" : int(term_freq_value),
            "es_id" : hit.meta.id
        })



    return flask.jsonify({"data" : output, "hits" : result.hits.total}) # , "hits" : num_hits


def testClustering():
    authorsOfInterest = ["StrindbergA", "AlmqvistCJL", "SoderbergH", "LagerlofS", "BoyeK", "MartinsonH", "BergmanHj", "AdlerbethGJ", "MolinL", "BauerJ", "MollerJensenE", "AtterbomPDA", "AroseniusI", "AdelborgO", "HornA", "SodergranE", "HeidenstamV", "BrennerSE", "SandelM", "HanssonGD"]
    # authorsOfInterest = ["StrindbergA"]

    for author in authorsOfInterest:
        body = {
            "query": {
                "more_like_this": {
                   "fields": [
                      "intro"
                   ],
                   "min_term_freq": 2,
                   "max_query_terms": 12,
                   "min_doc_freq" : 2,
                   # "max_doc_freq" : 5,
                   "like": [
                       {"_index": "litteraturbanken",
                    "_type": "author",
                    "_id": author}
                    ]
                }

            },
            "_source" : {
                "exclude" : ["text", "markup", "parts"],
                "include" : ["full_name"]
            }
        }

        data = es.search(index="litteraturbanken", doc_type="author", body=body, size=5)["hits"]
        print("__ " + author + " __")
        for hit in data["hits"]:
            print(hit["_source"]["full_name"].ljust(30), "%0.2f" % hit["_score"],)
        print("")



if __name__ == "__main__":
    # log all requests
    logger = logging.getLogger("werkzeug")

    fh = logging.FileHandler('logs/web.log', mode="w", encoding="UTF-8")
    fh.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)

    @app.errorhandler(500)
    def internal_server_error(error):
        print("********* error")
        return render_template('500.htm'), 500

    # logger.addHandler(fh)

    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(logging.NOTSET)

    app.logger.addHandler(fh)

    app.logger.setLevel(logging.DEBUG)
    app.logger.debug("ouch")
    app.run(debug=True, host='0.0.0.0')