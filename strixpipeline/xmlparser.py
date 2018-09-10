# coding=utf-8
import os
import re
import xml.etree.cElementTree as etree
from strixpipeline.config import config
import strixpipeline.mappingutil as mappingutil

os.environ["PYTHONIOENCODING"] = "utf_8"


def parse_pipeline_xml(file_name,
                       split_document,
                       word_annotations,
                       struct_annotations=(),
                       token_count_id=False,
                       text_attributes=None,
                       process_token=lambda x: None,
                       add_similarity_tags=False,
                       save_whitespace_per_token=False,
                       plugin=None,
                       pos_index_attributes=()):
    """
    split_document: everything under this node will go into separate documents
    word_annotations: a map of tag names and the attributes of those tags that
              will be included in annotation on word
    """
    if text_attributes is None:
        text_attributes = {}
    strix_parser = StrixParser(split_document, word_annotations, struct_annotations, token_count_id, text_attributes, process_token, add_similarity_tags, save_whitespace_per_token, plugin, pos_index_attributes)
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

    def __init__(self, split_document, word_annotations, struct_annotations, token_count_id, text_attributes, process_token, add_similarity_tags, save_whitespace_per_token, plugin, pos_index_attributes):
        #input
        self.split_document = split_document
        self.word_annotations = word_annotations
        self.struct_annotations = struct_annotations
        self.token_count_id = token_count_id
        self.text_attributes = text_attributes
        self.process_token = process_token
        self.add_similarity_tags = add_similarity_tags
        self.save_whitespace_per_token = save_whitespace_per_token
        self.plugin = plugin
        self.pos_index_attributes = pos_index_attributes

        #state
        self.current_part_tokens = []
        self.current_word_annotations = {}
        self.all_word_level_annotations = set()
        self.current_struct_annotations = {}

        self.current_token_lookup = []
        self.dump = [""]
        self.token_count = 0
        self.lines = [[0]]
        self.similarity_tags = []

        self.in_word = False
        self.word_attrs = {}
        self.current_word_content = ""

        self.current_parts = []

    def get_result(self):
        return self.current_parts

    def handle_starttag(self, tag, attrs):
        if self.text_attributes and tag == self.split_document:
            self.part_attributes = {}
            # TODO: don't loop through both text attributes and XML-node attributes
            for text_attr, text_attr_obj in self.text_attributes.items():
                for attribute in attrs:
                    if attribute == text_attr:
                        nodeName = attribute
                        newName = attribute
                    elif attribute == text_attr_obj.get("nodeName", None):
                        nodeName = attribute
                        newName = text_attr
                    else:
                        continue

                    text_attr_value = attrs[nodeName]
                    if self.text_attributes[newName].get("set", False):
                        text_attr_value = list(filter(bool, text_attr_value.split("|")))
                    if self.text_attributes[newName].get("type", "") == "double":
                        text_attr_value = "Infinity" if text_attr_value == "inf" else text_attr_value
                    self.part_attributes[newName] = text_attr_value

        elif tag == "w":
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

        elif tag != "w" and tag in self.word_annotations:
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
                if self.plugin:
                    self.plugin.process_text_attributes(self.part_attributes)
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
                res = mappingutil.token_separator.join(map(lambda x: x.get(key, mappingutil.empty_set), self.current_part_tokens))
                if key == "wid":
                    current_part["wid"] = res
                elif key in self.pos_index_attributes:
                    current_part["pos_" + key] = res

            if self.add_similarity_tags:
                current_part["similarity_tags"] = " ".join(self.similarity_tags)
            self.current_parts.append(current_part)

            self.token_count = 0
            self.current_part_tokens = []
            self.current_token_lookup = []
            self.current_word_annotations = {}
            self.current_struct_annotations = {}
            self.dump = [""]
            self.lines = [[0]]
            self.similarity_tags = []
            self.all_word_level_annotations = set()
        elif tag in self.struct_annotations:
            # at close we go thorugh each <w>-tag in the structural element and
            # assign the length (which can't be known until the element closes)
            # TODO do this once for ALL structural elements to avoid editing each token more than one
            #   (save all structs and do this when the document is done)
            annotation_length = self.current_struct_annotations[tag]["length"]

            for token in self.current_token_lookup[-annotation_length:]:
                token["attrs"][tag]["length"] = annotation_length
            del self.current_struct_annotations[tag]
        elif tag == "w":
            token_data = dict(self.current_word_annotations)
            for annotation in self.word_annotations.get("w", []):
                annotation_name = annotation["name"]
                if "nodeName" in annotation:
                    annotation_value = self.word_attrs.get(annotation["nodeName"])
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

            str_attrs = {}
            for attr, v in sorted(all_data.items()):
                if isinstance(v, list):
                    v = mappingutil.set_delimiter + mappingutil.set_delimiter.join(v) + mappingutil.set_delimiter if len(v) > 0 else mappingutil.set_delimiter
                if v is None:
                    v = mappingutil.empty_set
                str_attrs[attr] = str(v)

            token = self.current_word_content.strip()
            self.dump[-1] += token
            str_attrs["token"] = token
            self.current_part_tokens.append(str_attrs)

            token_lookup_data = dict(token_data)
            token_lookup_data.update(struct_annotations)
            self.current_token_lookup.append({"word": token, "attrs": token_lookup_data, "position": self.token_count})

            self.token_count += 1

            if self.add_similarity_tags and token_data["pos"] == "NN":
                if "lemma" in token_data:
                    annotation_value = token_data["lemma"]
                else:
                    annotation_value = [lemma for lemma in self.word_attrs["lemma"].split("|") if lemma and ":" not in lemma]
                if not annotation_value:
                    annotation_value = [token]
                self.similarity_tags.extend(annotation_value)
            self.in_word = False
            self.current_word_content = ""

    def handle_data(self, data):
        if self.in_word:
            self.current_word_content += data.strip()
        else:
            whitespaces = data.splitlines(True)
            for ws in whitespaces:
                self.dump[-1] += ws
                if self.save_whitespace_per_token:
                    try:
                        self.current_token_lookup[-1]["whitespace"] = ws
                    except IndexError:
                        pass
                if ws[-1] == "\n":
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
                strix_parser.handle_data(element.text)
            strix_parser.handle_endtag(element.tag)
            if element.tail:
                strix_parser.handle_data(element.tail)
            root.clear()

        if event == "start":
            strix_parser.handle_starttag(element.tag, element.attrib)