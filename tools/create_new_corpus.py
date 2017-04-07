import requests
import json
import sys


types = {"lex": "lemgram", "prefix": "lemgram", "suffix": "lemgram"}
set_attrs = ["lemma", "lex", "sense", "prefix", "suffix", "compwf", "complemgram"]


def create_new_corpus(corpus):
    response = requests.get("https://spraakbanken.gu.se/eng/resource/{}/json".format(corpus))
    corpus_metadata = response.json()

    id_info = corpus_metadata["metadata"]["identificationInfo"]
    corpus_id = id_info["identifier"]
    corpus_name = id_info["resourceName"]
    corpus_description = id_info["description"]

    word_attributes = []
    for pos_attribute in corpus_metadata["korpInfo"]["attrs"]["p"]:
        print("Adding {}".format(pos_attribute))
        if pos_attribute == "word":
            continue
        if pos_attribute == "lex":
            name = "lemgram"
        else:
            name = pos_attribute
        word_attribute = {
            "name": name,
            "set": pos_attribute in set_attrs,
        }
        if pos_attribute == "lex":
            word_attribute["nodeName"] = "lex"
        if pos_attribute in types:
            word_attribute["type"] = types[pos_attribute]
        word_attributes.append(word_attribute)

    document_id = ""
    text_attributes = []
    struct_attributes = {}
    for attribute in corpus_metadata["korpInfo"]["attrs"]["s"]:
        if "_" in attribute:
            struct = attribute.split("_")[0]
            attr = "_".join(attribute.split("_")[1:])

            if struct == "text":
                print("Adding {} to text_attributes".format(attr))
                text_attributes.append({
                    "name": attr
                })
                if attr == "title":
                    document_id = "title"
                if document_id != "title" and attr == "titel":
                    document_id = "titel"
            else:
                if struct in ["sentence","paragraph","dokument"]:
                    continue
                print("Adding {}_{} to struct_attributes".format(struct, attr))
                if struct not in struct_attributes:
                    struct_attributes[struct] = []

                if attr.startswith("_"):
                    name = attr[1:]
                else:
                    name = attr
                struct_attr = {
                    "name": name,
                    "set": False
                }
                if attr.startswith("_"):
                    struct_attr["nodeName"] = attr
                struct_attributes[struct].append(struct_attr)

    res = {
        "corpus_id": corpus_id,
        "corpus_name": corpus_name,
        "corpus_description": corpus_description,
        "file_name_pattern": "*.xml",
        "analyze_config": {
            "word_attributes": word_attributes,
            "struct_attributes": struct_attributes,
            "text_attributes": text_attributes
        },
        "document_id": document_id,
        "title": {
             "pattern": "{" + document_id + "}",
             "keys": [document_id]
         },
        "parser": "htmlparser",
        "translation": {}
    }

    fp = open("../resources/config/{}.json".format(corpus_id), "w")
    json.dump(res, fp, indent=4, ensure_ascii=False)

if __name__ == '__main__':
    for corpus in sys.argv[1:]:
        print("Creating file for %s" % corpus)
        create_new_corpus(corpus)

    print("Done.")
