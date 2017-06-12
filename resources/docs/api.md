# Strix web service version 0.0.1

## About

### Documents

#### Common fields

Each document have the following fields in common:

* **dump** - The whole text as an array with whitespace preserved and each line as an element in the array
* **word_count** - The number of tokens in the text
* **lines** - For each line, the position of the first and last token on the line
* **title** - The title of the document
* **text_attributes** - The text attributes of the document
* **token_lookup** - Each token of the document along with word attributes
* All text attributes that are available for the selected corpus, given by the `/config/<corpora>` call.

#### include / exclude

For every API call returning a document it is possible to control which of these fields should be included 
in the result. This is done using the `include` and `exclude` query parameters.

`include` - Comma-separated list of fields. Only the given fields will be returned. Possible values are the common fields.  
`exclude` - Comma-separated list of fields. All fields except the given fields will be returned. Possible values are the common fields.

#### token_lookup

For every API call returning a document it is possible to control the size of the `token_lookup`:

`token_lookup_from` - Position / WID  
`token_lookup_to` - Position / WID

Note that the returned array can be huge depending on document size and may take a long time to fetch. 
For calls returning multiple documents (search and related documents), it is disabled by default.
It is also possible to use `include` or `exclude` for `token_lookup`.

#### Aggregations

## Requests

### Get a document

**GET** `/document/<corpus>/<doc_id>`

Returns a document from `corpus` with `doc_id` as the document ID

**Supported query params:**

* `include`
* `exclude`


### Search for documents

**GET** `/search`

Search in the given `corpora` using `text_query` for in-text-search and `text_filter`,
for material filtering. When `text_query_field` is empty, the given `text_query` will be 
tokenized, lemmatized and searched for in the text content. When `text_query_field` is non-empty,
search in word-level-annotations will be performed (see `/config/<corpora>` for supported annotations).

When `text_query` is empty, all documents in current selection will be returned.

**Supported query params:**

* `corpora` - Selected material. (default: All corpora)
* `include`
* `exclude`
* `token_lookup_from`
* `token_lookup_to`
* `from` - For pagination through the results. (default: 0) 
* `to` - For pagination through the results. Must be larger than `from`. Cannot be larger than 10000. (default: 10)
* `highlight` - boolean, should the matching tokens be returned or not (default: true)
* `highlight_number_of_fragments` - how many of the matching tokens should be returned (default: 5)
* `text_query` - Search string.
* `text_query_field` - Field to search for `text_query` in.
* `text_filter` - JSON formatted search query. Use same structure as given by `/config/<corpora>` (default: no filter)  
   Example1: `text_filter={ "party": "v", "year": "2010" }`  
   Example2: `text_filter={ "datefrom": { "range": { "gte": "19900101","lte": "19960101" }}}`

### Search in document

**GET** `/search/<corpus>/doc_id/<doc_id>/`

Search in the selected  document using `text_query` for in-text-search. When `text_query_field` is empty, 
the given `text_query` will be tokenized, lemmatized and searched for in the text content. When `text_query_field` is non-empty,
search in word-level-annotations will be performed (see `/config/<corpora>` for supported annotations).

If `text_query` is empty, the document will be returned without highlights.

**Supported query params:**

* `corpora` - Selected material. (default: All corpora)
* `include`
* `exclude`
* `token_lookup_from`
* `token_lookup_to`
* `text_query` - Search string.
* `text_query_field` - Field to search for `text_query` in.
* `size` - The number of hits to return (default: all hits)
* `current_position` - The position to start looking for hits from (default: start of document)
* `forward` - boolean, search for matches forward or backward in the document (default: true)

### Get all possible values for a field

**GET** `/field_values/<corpus>/<field>`

For any text field (available fields  given by `/config/<corpora>`) return all possible values of that field.

### Get configuration

**GET** `/config`

Returns the available corpora.

**GET** `/config/<corpora>`

`corpora` is a comma-separated list. Returns the configuration for all given corpora.

### Get related documents

**GET** `/related/<corpus>/doc_id/<doc_id>`

Get a list of documents that are related to the given document.

For information about the `more_like_this` functionality, see:
https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-mlt-query.html

**Supported query params:**

* `include`
* `exclude`
* `token_lookup_from`
* `token_lookup_to`
* `from` - For pagination through the results (default: 0) 
* `to` - For pagination through the results (default: 10)
* `search_corpora` - List of corpora to search for related documents in (default: same corpus as query document)
* `relevance_function` - Possible values are: `more_like_this`, `disjunctive_query` (default: `more_like_this`)
* `max_query_terms` - Only applicable for `more_like_this`, see elasticsearch documentation (default: 30)
* `min_term_freq` - Only applicable for `more_like_this`, see elasticsearch documentation (default: 1)