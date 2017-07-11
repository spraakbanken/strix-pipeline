import requests
import logging

_logger = logging.getLogger(__name__)


def lemgrammify(terms):
    response = requests.get("https://ws.spraakbanken.gu.se/ws/karp/v4/autocomplete?mode=external&multi=" + ",".join(terms) + "&resource=saldom")
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        message = "Unable to use Karp autocomplete service"
        _logger.exception(message)
        raise RuntimeError(message)
    res_lemgrams = {}
    result = response.json()
    for term in terms:
        lemgrams = []
        if term in result:
            for hit in result[term]["hits"]["hits"]:
                lemgram = hit["_source"]["FormRepresentations"][0]["lemgram"]
                if "_" not in lemgram[1:]:
                    lemgrams.append(lemgram.lower())  # .lower() here is because we accidentally have lowercase active in the mapping
        res_lemgrams[term] = lemgrams
    return res_lemgrams
