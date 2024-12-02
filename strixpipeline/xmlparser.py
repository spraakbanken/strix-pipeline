import re
import xml.etree.cElementTree as etree
import strixpipeline.mappingutil as mappingutil
from collections import Counter


def parse_pipeline_xml(
    file_name,
    split_document,
    word_annotations,
    struct_annotations=(),
    token_count_id=False,
    text_attributes=None,
    process_token=lambda x: None,
    add_most_common_words=False,
    save_whitespace_per_token=False,
    pos_index_attributes=(),
    text_tags=None,
):
    """
    split_document: everything under this node will go into separate documents
    word_annotations: a map of tag names and the attributes of those tags that
              will be included in annotation on word
    """
    if text_attributes is None:
        text_attributes = {}
    if text_tags is None:
        text_tags = []
    strix_parser = StrixParser(
        split_document,
        word_annotations,
        struct_annotations,
        token_count_id,
        text_attributes,
        process_token,
        add_most_common_words,
        save_whitespace_per_token,
        pos_index_attributes,
        text_tags,
    )
    iterparse_parser(file_name, strix_parser)
    res = strix_parser.get_result()
    return res


def parse_properties(annotation, in_value):
    out_value = {}
    for prop_name, prop in annotation["properties"].items():
        if "properties" in prop:
            out_value[prop_name] = parse_properties(prop, in_value)
        else:
            out_value[prop_name] = re.search(prop["value"], in_value).group(1)
    return out_value


class StrixParser:
    def __init__(
        self,
        split_document,
        word_annotations,
        struct_annotations,
        token_count_id,
        text_attributes,
        process_token,
        add_most_common_words,
        save_whitespace_per_token,
        pos_index_attributes,
        text_tags,
    ):
        # input
        self.split_document = split_document
        self.word_annotations = word_annotations
        self.struct_annotations = struct_annotations
        self.token_count_id = token_count_id
        self.text_attributes = text_attributes
        self.process_token = process_token
        self.add_most_common_words = add_most_common_words
        self.save_whitespace_per_token = save_whitespace_per_token
        self.pos_index_attributes = pos_index_attributes
        self.text_tags = text_tags

        # state
        self.current_part_tokens = []
        self.current_word_annotations = {}
        self.all_word_level_annotations = set()
        self.current_struct_annotations = {}

        self.current_token_lookup = []
        self.dump = [""]
        self.token_count = 0
        self.lines = [[0]]
        self.most_common_words = []
        self.ner_tags = []
        self.geo_locations = []

        self.in_word = False
        self.word_attrs = {}
        self.current_word_content = ""

        self.current_parts = []
        self.start_tag = ""

    def get_result(self):
        return self.current_parts

    def handle_starttag(self, tag, attrs):
        if self.text_attributes and tag in self.text_tags:
            if not self.start_tag:
                self.start_tag = tag
                self.upper_level = {}
            if tag == self.split_document:
                self.part_attributes = {}
                for text_attr, text_attr_obj in self.text_attributes.items():
                    for attribute in attrs:
                        if attribute == text_attr:
                            node_name = attribute
                            new_name = attribute
                        elif attribute == text_attr_obj.get("nodeName", None):
                            node_name = attribute
                            new_name = text_attr
                        else:
                            continue

                        text_attr_value = attrs[node_name]
                        if self.text_attributes[new_name].get("set", False) or (
                            text_attr_value[0] == "|" and text_attr_value[-1] == "|"
                        ):
                            text_attr_value = list(filter(bool, text_attr_value.split("|")))
                        if self.text_attributes[new_name].get("type", "") == "double":
                            text_attr_value = "Infinity" if text_attr_value == "inf" else text_attr_value
                        self.part_attributes[new_name] = text_attr_value
                for key, value in self.upper_level.items():
                    self.part_attributes[key] = value
            if tag != self.split_document:
                for text_attr, text_attr_obj in self.text_attributes.items():
                    for attribute in attrs:
                        if tag + "_" + attribute == text_attr:
                            node_name = attribute
                            new_name = text_attr
                        elif tag + "_" + attribute == text_attr_obj.get("nodeName", None):
                            node_name = attribute
                            new_name = text_attr
                        else:
                            continue

                        text_attr_value = attrs[node_name]
                        if self.text_attributes[new_name].get("set", False) or (
                            text_attr_value[0] == "|" and text_attr_value[-1] == "|"
                        ):
                            text_attr_value = list(filter(bool, text_attr_value.split("|")))
                        if self.text_attributes[new_name].get("type", "") == "double":
                            text_attr_value = "Infinity" if text_attr_value == "inf" else text_attr_value
                        self.upper_level[new_name] = text_attr_value
        elif tag == "token":
            self.in_word = True
            self.word_attrs = attrs

        elif tag in self.struct_annotations:
            self.current_struct_annotations[tag] = {"attrs": {}}
            annotations = self.struct_annotations[tag]

            for annotation in annotations:
                annotation_name = annotation["name"]
                if "nodeName" in annotation:
                    a_value = attrs[annotation["nodeName"]]
                elif annotation_name in attrs:
                    a_value = attrs[annotation_name]
                else:
                    break

                if annotation.get("set", False):
                    a_value = list(filter(bool, a_value.split("|")))

                if "properties" in annotation:
                    if annotation.get("set", False):
                        a_value = [parse_properties(annotation, x) for x in a_value]
                    else:
                        a_value = parse_properties(annotation, a_value)

                self.current_struct_annotations[tag]["attrs"][annotation_name] = a_value

        elif tag != "token" and tag in self.word_annotations:
            annotations = self.word_annotations[tag]

            for annotation in annotations:
                annotation_name = annotation["name"]
                if "nodeName" in annotation:
                    a_value = attrs[annotation["nodeName"]]
                else:
                    a_value = attrs[annotation_name]

                self.current_word_annotations[annotation_name] = a_value

    def handle_endtag(self, tag):
        if tag == self.split_document:
            current_part = {}
            if self.text_attributes:
                if "year" not in self.part_attributes.keys():
                    # TODO do not augment data inside XML-parser
                    date_from = ""
                    date_to = ""
                    given_date = ""
                    if "datefrom" in self.part_attributes.keys():
                        date_from = self.part_attributes["datefrom"][0:4]
                    if "dateto" in self.part_attributes.keys():
                        date_to = self.part_attributes["dateto"][0:4]

                    if "date" in self.part_attributes.keys():
                        given_date = self.part_attributes["date"][0:4]
                    elif "datum" in self.part_attributes.keys():
                        given_date = self.part_attributes["datum"][0:4]
                    elif "topic_year" in self.part_attributes.keys():
                        given_date = self.part_attributes["topic_year"]

                    # TODO find permanent solution
                    if not date_from and (not date_to and (not given_date)):
                        self.part_attributes["year"] = "2050"
                    elif given_date:
                        self.part_attributes["year"] = given_date
                    elif date_to and not date_from:
                        self.part_attributes["year"] = date_to
                    elif date_from and not date_to:
                        self.part_attributes["year"] = date_from
                    elif date_from == date_to:
                        self.part_attributes["year"] = date_from
                    elif date_from != date_to:
                        self.part_attributes["year"] = date_from + ", " + date_to

                for key, val in self.part_attributes.items():
                    if key in self.text_attributes and self.text_attributes[key].get("index", True):
                        current_part["text_" + key] = val
                current_part["text_attributes"] = self.part_attributes

            current_part["token_lookup"] = self.current_token_lookup

            if len(self.lines[-1]) == 1 and self.lines[-1][0] != -1:
                self.lines[-1] = [self.lines[-1][0], self.token_count - 1]
            current_part["dump"] = self.dump
            current_part["lines"] = self.lines

            current_part["word_count"] = len(self.current_part_tokens)

            current_part["text"] = mappingutil.token_separator.join(map(lambda x: x["token"], self.current_part_tokens))

            for key in self.all_word_level_annotations:
                res = mappingutil.token_separator.join(
                    map(
                        lambda x: x.get(key, mappingutil.empty_set),
                        self.current_part_tokens,
                    )
                )
                if key == "wid":
                    current_part["wid"] = res
                elif key in self.pos_index_attributes:
                    current_part["pos_" + key] = res

            if self.ner_tags:
                current_part["ner_tags"] = ", ".join(
                    [
                        key + " (" + str(value) + ")"
                        for key, value in dict(
                            Counter([i for i in self.ner_tags if len(i) > 3]).most_common(10)
                        ).items()
                    ]
                )

            if self.geo_locations:
                current_part["geo_location"] = self.geo_locations

            if self.add_most_common_words:
                current_part["most_common_words"] = ", ".join(
                    [
                        key + " (" + str(value) + ")"
                        for key, value in dict(
                            Counter([i for i in self.most_common_words if len(i) > 3]).most_common(20)
                        ).items()
                    ]
                )
            self.current_parts.append(current_part)

            self.token_count = 0
            self.current_part_tokens = []
            self.current_token_lookup = []
            self.current_word_annotations = {}
            self.current_struct_annotations = {}
            self.dump = [""]
            self.lines = [[0]]
            self.most_common_words = []
            self.ner_tags = []
            self.geo_locations = []
            self.all_word_level_annotations = set()
            # self.start_tag = ""
        elif tag in self.struct_annotations:
            # at close we go thorugh each <w>-tag in the structural element and
            # assign the length (which can't be known until the element closes)
            # TODO do this once for ALL structural elements to avoid editing each token more than one
            #   (save all structs and do this when the document is done)
            if "length" in self.current_struct_annotations[tag]:
                annotation_length = self.current_struct_annotations[tag]["length"]
                for token in self.current_token_lookup[-annotation_length:]:
                    token["attrs"][tag]["length"] = annotation_length
            del self.current_struct_annotations[tag]
        elif tag == "token":
            token = self.current_word_content.strip()
            if len(token) != 0:
                token_data = dict(self.current_word_annotations)
                for annotation in self.word_annotations.get("token", []):
                    annotation_name = annotation["name"]
                    if "nodeName" in annotation:
                        annotation_value = []
                        for lemma in self.word_attrs.get(annotation["nodeName"]).split("|"):
                            if lemma and (":" not in lemma):
                                annotation_value.append(lemma)
                            elif lemma and (":" in lemma):
                                annotation_value.append(lemma.split(":")[0])
                            else:
                                pass
                        if annotation_value:
                            annotation_value = "|".join(annotation_value)
                        else:
                            annotation_value = ""
                    else:
                        annotation_value = self.word_attrs.get(annotation_name)
                    if annotation.get("set", False) or annotation.get("ranked", False):
                        annotation_value = list(filter(bool, annotation_value.split("|")))
                    if annotation.get("ranked", False):
                        values = [v.split(":")[0] for v in annotation_value]
                        annotation_value = values[0] if values else None
                    token_data[annotation_name] = annotation_value
                    self.all_word_level_annotations.add(annotation_name)

                if self.token_count_id:
                    token_data["wid"] = self.token_count
                    self.all_word_level_annotations.add("wid")

                struct_data = {}
                struct_annotations = {}
                for tag_name, annotations in self.current_struct_annotations.items():
                    struct_annotations[tag_name] = {"attrs": annotations["attrs"]}
                    if "start_wid" not in annotations:
                        annotations["start_wid"] = token_data["wid"]
                        annotations["start_pos"] = self.token_count
                        struct_annotations[tag_name]["is_start"] = True
                    struct_annotations[tag_name]["start_wid"] = annotations["start_wid"]
                    annotations["length"] = self.token_count - annotations["start_pos"] + 1

                    if "attrs" in annotations:
                        for annotation_name, v in annotations["attrs"].items():
                            x = tag_name + "_" + annotation_name
                            struct_data[x] = v
                            self.all_word_level_annotations.add(x)

                self.process_token(token_data)
                all_data = dict(token_data)
                all_data.update(struct_data)

                if "ne_name" in struct_data.keys():
                    if struct_data["ne_type"] != "MSR" and (struct_data["ne_type"] != "TME"):
                        self.ner_tags.append(struct_data["ne_name"])
                if "sentence__geocontext" in struct_data:
                    locations = struct_data["sentence__geocontext"].split("|")[1:-1]
                    self.geo_locations.extend(locations)

                str_attrs = {}
                for attr, v in sorted(all_data.items()):
                    if isinstance(v, list):
                        v = (
                            mappingutil.set_delimiter + mappingutil.set_delimiter.join(v) + mappingutil.set_delimiter
                            if len(v) > 0
                            else mappingutil.set_delimiter
                        )
                    if v is None:
                        v = mappingutil.empty_set
                    str_attrs[attr] = str(v)

                self.dump[-1] += token
                str_attrs["token"] = token
                self.current_part_tokens.append(str_attrs)

                token_lookup_data = dict(token_data)
                token_lookup_data.update(struct_annotations)
                self.current_token_lookup.append(
                    {
                        "word": token,
                        "attrs": token_lookup_data,
                        "position": self.token_count,
                    }
                )

                self.token_count += 1

                if self.add_most_common_words and token_data["pos"] == "NN":
                    if "lemma" in token_data:
                        annotation_value = [
                            lemma for lemma in token_data["lemma"] if ":" not in lemma and ("--" not in lemma)
                        ]
                    else:
                        annotation_value = [
                            lemma
                            for lemma in self.word_attrs.get("lemma", "").split("|")
                            if lemma and (":" not in lemma and ("--" not in lemma))
                        ]
                    if not annotation_value:
                        annotation_value = [token]
                    self.most_common_words.extend(annotation_value)
            self.in_word = False
            self.current_word_content = ""

    def handle_data(self, data, tag_value):
        if self.in_word:
            self.current_word_content += data.strip()
        else:
            if tag_value == "token":
                whitespaces = (
                    self.word_attrs.get("_tail", "").replace("\\s", " ").replace("\\n", "y").replace("\\t", "x")
                )
                whitespaces = whitespaces.replace("x", "").replace("y", "\n")
                for ws in whitespaces:
                    self.dump[-1] += ws
                    if self.save_whitespace_per_token:
                        try:
                            self.current_token_lookup[-1]["whitespace"] = ws
                        except IndexError:
                            pass
                    if "\n" in ws[-1]:
                        current_token = self.token_count - 1
                        self.dump.append("")
                        [begin] = self.lines[-1]
                        if begin == current_token + 1:
                            self.lines[-1] = [-1]
                        else:
                            self.lines[-1] = [begin, current_token]
                        self.lines.append([current_token + 1])


def iterparse_parser(file_name, strix_parser):
    book_iter = etree.iterparse(file_name, events=("start", "end"))

    # solution from http://infix.se/2009/05/10/text-safe-xml-processing-with-iterparse
    def delayediter(iterable):
        iterable = iter(iterable)
        prev = next(iterable)
        for item in iterable:
            yield prev
            prev = item
        yield prev

    book_iter = delayediter(book_iter)

    _, root = next(book_iter)
    strix_parser.handle_starttag(root.tag, root.attrib)
    for event, element in book_iter:
        if event == "end":
            if element.text:
                strix_parser.handle_data(element.text, element.tag)
            strix_parser.handle_endtag(element.tag)
            if element.tail:
                strix_parser.handle_data(element.tail, element.tag)
            root.clear()

        if event == "start":
            strix_parser.handle_starttag(element.tag, element.attrib)
