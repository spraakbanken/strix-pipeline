# -*- coding: utf-8 -*-
import flask
import elasticsearch
from functools import update_wrapper
from datetime import timedelta
from flask import Flask, make_response, request, current_app
import logging, sys, re
from elasticsearch_dsl import Search, Q
from functools import wraps
import strix.config as config
from pprint import pprint
app = Flask(__name__)
es = elasticsearch.Elasticsearch(config.elastic_hosts)


# Decorator for allowing cross site http request etc.
# http://flask.pocoo.org/snippets/56/
def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is None:
        # Set standard headers here
        # TODO figure out which ones that are meaningful (when)
        headers = ['Content-Type','Authorization']
    if headers is not None and not isinstance(headers, str):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, str):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        f.required_methods = ['OPTIONS']
        return update_wrapper(wrapped_function, f)
    return decorator


def jsonify_response(f):
    """decorator for jsonifying a elasticsearch-dsl response type with flask"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        res = f(*args, **kwargs)
        return flask.jsonify({"data" : [x.to_dict() for x in res]})

    return wrapper


def check_elastic_search_dsl_version():
    try:
        Search().source
    except AttributeError as e:
        print("""Using untagged features of the elasticsearch-dsl lib, install using
             pip install https://github.com/elastic/elasticsearch-dsl-py/archive/master.zip """,
             file=sys.stderr) # error valid for elasticsearch-dsl version 2.0.0
        raise e


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


@app.route("/document/<corpus>/<doc_type>/<doc_id>")
@crossdomain(origin='*')
def get_document(corpus, doc_type, doc_id):
    includes, excludes = get_includes_excludes()
    return flask.jsonify(es.get(index=corpus, doc_type=doc_type, id=doc_id,
                                _source_include=includes, _source_exclude=excludes)['_source'])


@app.route("/document/<corpus>/<doc_type>/<from_page>/<to_page>")
@crossdomain(origin='*')
@jsonify_response
def get_documents(corpus, doc_type, from_page, to_page):
    check_elastic_search_dsl_version()
    includes, excludes = get_includes_excludes()
    s = Search(using=es, index=corpus, doc_type=doc_type)
    s = s.source(include=includes, exclude=excludes)
    hits = s[int(from_page):int(to_page)].execute()
    for hit in hits:
        hit['es_id'] = hit.meta.id
    return hits


@app.route("/search/<corpus>/<doc_type>/<search_field>/<search_term>")
@crossdomain(origin='*')
@jsonify_response
def search(corpus, doc_type, search_field, search_term):
    check_elastic_search_dsl_version()
    includes, excludes = get_includes_excludes()

    # how to exclude for example vivill
    # s = Search(using=es, index='litteraturbanken,-*vivill', doc_type=doc_type) \
    s = Search(using=es, index=corpus, doc_type=doc_type) \
        .query("match", **{search_field: search_term}) \
        .highlight(search_field, fragment_size=18) \
        .source(include=includes, exclude=excludes)
    hits = s.execute()
    result = []
    for hit in hits:
        hit['highlight'] = hit.meta.highlight.to_dict()
        hit['es_id'] = hit.meta.id
        result.append(hit)
    return result


#showFields: array av nycklar från författartypen som ska med tillbaka, dvs getAuthors(["birthYear", "nameforindex"])
@app.route("/get_authors")
@crossdomain(origin='*')
def getAuthors(): #arg: showFields
    # denna pratar nog med en mer generell strix-apivariant. 

    body = {
        "sort" : [
            { "name_for_index" : "asc" }
        ],
        "query": {
            "type" : {
                "value" : "author"
            }
        }
        # "_source": ["full_name", "birth", "name_for_index","gender", "author_id"]

    }

    data = es.search(index="litteraturbanken", doc_type="author", body=body, size=10000)["hits"]
    # print(json.dumps(data))

    output = []
    for obj in data["hits"]:
        auth = obj["_source"]
        auth["fullname"] = auth.pop("full_name")
        auth["nameforindex"] = auth.pop("name_for_index")
        auth["authorid"] = auth.pop("author_id")
        output.append(auth)

    return flask.jsonify({"data" : output})


@app.route("/get_lb_author/<authorid>")
@crossdomain(origin='*')
def getAuthor(authorid):
    includes, excludes = get_includes_excludes()
    response = Search(using=es, index="litteraturbanken", doc_type="author") \
        .filter("term", author_id=authorid) \
        .source(include=includes, exclude=excludes) \
        .execute()

    return flask.jsonify({"data" : response[0].to_dict()})

# types[]: lista av 'work' eller 'title'
# authors[]: lista av authorid 
# filterString: delsträngsfiltrering på titel och author
@app.route("/lb_list_all/<types>")
@app.route("/lb_list_all/<types>/<authors>")
@crossdomain(origin="*")
@jsonify_response
def listAll(types, authors=""):
    """
    query param: filter_string for filtering on relevant fields
    """
    includes, excludes = get_includes_excludes()

    filter_string = request.args.get("filter_string", "")

    s = Search(using=es, index="litteraturbanken", doc_type=types)
    if authors:
        q = Q("term", **{"authors.author_id" : authors.split(",")[0]})
        for auth in authors.split(",")[1:]:
            q = q | Q("term", **{"authors.author_id" : auth})

        s.query = Q('nested', path="authors", query=q)
    elif filter_string:
        q = Q("wildcard", **{"authors.full_name" : "*" + filter_string + "*"})
        
        s.query = Q('nested', path="authors", query=q) | Q('wildcard', title="*" + filter_string + "*")



    # print("query", s.to_dict())
    pprint(s.to_dict())
    return s.source(include=includes, exclude=excludes) \
    .execute()



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
def getEpub():
    check_elastic_search_dsl_version()

    search = Search(using=es, index="litteraturbanken") \
        .filter("exists", field="epub")

    if request.args.get("include"):
        search = search.source(include=request.args.get("include").split(","))

    return search[0:int(request.args.get("size", 20))].execute()


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
        .bucket("authorid", "terms", field="authors.authorid", size=0) \
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
                "match": formatContext(re.search(match, row).groups()[0].split("||")),
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


