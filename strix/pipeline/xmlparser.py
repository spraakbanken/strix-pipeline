# coding=utf-8
import os
import re
import xml.etree.cElementTree as etree
from html.parser import HTMLParser
import logging

os.environ["PYTHONIOENCODING"] = "utf_8"

logger = logging.getLogger(__name__)


namespaces = {"xml": "http://www.w3.org/XML/1998/namespace"}


def xml_to_json(xml_root, parse_as_list=(), parse_as_sublist=(), parse_as_bool=(), parse_as_inner_xml=(),
                parse_as_list_of_mappings=(), parse_attributes=(), nodename_translate={}, ignore=(),
                nodename_translate_function=lambda x: x):
    """
    parse_as_list: multiple elems with same tagname on root level
    parse_as_bool: self explanatory
    parse_as_sublist: list of elements containing html under common parent, e.g
        <sources>
          <source>html <i>content</i></source>
        </sources>
    parse_as_list_of_mappings: subnodes of each matched element are considered key-value pairs
    parse_as_inner_xml: self explanatory
    ignore: skip parsing of these nodenames
    """

    def xml_to_obj(node):
        d = dict(node.attrib)
        d[node.tag] = node.text
        return d

    def xml_children_to_obj(node):
        return {child.tag: child.text for child in node}

    nodemap = {}
    for child in xml_root:
        if child.tag in ignore:
            continue
        val = None
        if child.tag in parse_as_list:
            l = nodemap.get(nodename_translate_function(nodename_translate.get(child.tag, child.tag)), [])
            l.append(xml_to_obj(child))
            val = l
        elif child.tag in parse_as_sublist:
            output = []
            for subnode in child:
                # get inner html
                nodestr = etree.tostring(subnode, encoding="unicode").strip()
                tag = subnode.tag
                nodestr = re.sub("^<%s*.?>" % tag, "", nodestr)
                nodestr = re.sub("</%s>$" % tag, "", nodestr)
                output.append(nodestr)

            val = output
        elif child.tag in parse_as_list_of_mappings:
            l = nodemap.get(nodename_translate_function(nodename_translate.get(child.tag, child.tag)), [])
            l.append({nodename_translate_function(nodename_translate.get(subnode.tag, subnode.tag)): subnode.text for subnode in child})
            val = l
            
        elif child.tag in parse_as_bool:
            val = child.text == "true"
        elif child.tag in parse_as_inner_xml:
            if len(child) or (child.text or "").strip():  # Only if tag is not empty
                # print(child)
                val = etree.tostring(child, encoding="unicode").strip()
                # val = etree.tostring(child).strip()
            else:
                val = None
        elif child.tag in parse_attributes:
            val = {"text": child.text}
            for attr_name, attr_value in child.attrib.items():
                val[attr_name] = attr_value
        else:
            val = child.text

        key = nodename_translate_function(nodename_translate.get(child.tag, child.tag))
        nodemap[key] = val

    return nodemap


def parse_pipeline_xml(file_name,
                       split_document,
                       word_annotations,
                       parser=None,
                       struct_annotations=(),
                       token_count_id=False,
                       text_attributes=None,
                       process_token=lambda x: None,
                       add_similarity_tags=False):
    """
    split_document: everything under this node will go into separate documents
    word_annotations: a map of tag names and the attributes of those tags that
              will be included in annotation on word
    """
    if text_attributes is None:
        text_attributes = {}
    strix_parser = StrixParser(split_document, word_annotations, struct_annotations, token_count_id, text_attributes, process_token, add_similarity_tags)
    if parser == "htmlparser":
        strixHTMLParser = StrixHTMLParser(strix_parser)

        def read_in_chunks(file_object, chunk_size=1024):
            """Lazy function (generator) to read a file piece by piece.
            Default chunk size: 1k."""
            while True:
                data = file_object.read(chunk_size)
                if not data:
                    break
                yield data

        with open(file_name, "r") as file:
            for piece in read_in_chunks(file):
                strixHTMLParser.feed(piece)
    else:
        iterparse_parser(file_name, strix_parser)
    res = strix_parser.get_result()
    return res


class StrixParser:

    def __init__(self, split_document, word_annotations, struct_annotations, token_count_id, text_attributes, process_token, add_similarity_tags):
        #input
        self.split_document = split_document
        self.word_annotations = word_annotations
        self.struct_annotations = struct_annotations
        self.token_count_id = token_count_id
        self.text_attributes = text_attributes
        self.process_token = process_token
        self.add_similarity_tags = add_similarity_tags

        #state
        self.current_part_tokens = []
        self.current_word_annotations = {}
        self.current_struct_annotations = {}

        self.current_token_lookup = []
        self.dump = [""]
        self.token_count = 0
        self.lines = [[0]]
        self.similarity_tags = []

        self.in_word = False
        self.word_attrs = {}

        self.current_parts = []

    def get_result(self):
        return self.current_parts

    def handle_starttag(self, tag, attrs):
        if self.text_attributes and tag == self.split_document:
            self.part_attributes = {}
            for attribute in attrs:
                if attribute in self.text_attributes:
                    self.part_attributes[attribute] = attrs[attribute]

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
                else:
                    a_value = attrs[annotation_name]

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
            if self.text_attributes:
                current_part = self.part_attributes
            else:
                current_part = {}
            current_part["token_lookup"] = self.current_token_lookup

            if len(self.lines[-1]) == 1 and self.lines[-1][0] != -1:
                self.lines[-1] = [self.lines[-1][0], self.token_count - 1]
            current_part["dump"] = self.dump
            current_part["lines"] = self.lines

            current_part["word_count"] = len(self.current_part_tokens)
            current_part["text"] = "\u241D".join(self.current_part_tokens)
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
        elif tag in self.struct_annotations:
            del self.current_struct_annotations[tag]
        elif tag == "w":
            self.in_word = False

    def handle_data(self, data):
        if self.in_word:
            token_data = dict(self.current_word_annotations)
            for annotation in self.word_annotations.get("w", []):
                annotation_name = annotation["name"]
                if "nodeName" in annotation:
                    annotation_value = self.word_attrs.get(annotation["nodeName"])
                else:
                    annotation_value = self.word_attrs.get(annotation_name)
                if annotation["set"]:
                    annotation_value = list(filter(bool, annotation_value.split("|")))
                token_data[annotation_name] = annotation_value

            if self.token_count_id:
                token_data["wid"] = self.token_count

            struct_data = {}
            for tag_name, annotations in self.current_struct_annotations.items():
                if "length" not in annotations:
                    annotations["length"] = 1
                else:
                    offset = -annotations["length"]
                    start_token_data = self.current_token_lookup[offset]["attrs"]
                    start_token_data[tag_name]["length"] += 1
                    self.current_struct_annotations[tag_name]["start_wid"] = start_token_data["wid"]

                for k, attr in annotations.items():
                    if k == "attrs":
                        for annotation_name, v in attr.items():
                            struct_data[tag_name + "_" + annotation_name] = v

            self.process_token(token_data)
            all_data = dict(token_data)
            all_data.update(struct_data)

            str_attrs = []
            for attr, v in sorted(all_data.items()):
                if isinstance(v, list):
                    v = "\u241F" + "\u241F".join(v) + "\u241F" if len(v) > 0 else "\u241F"
                str_attrs.append(attr + "=" + str(v))

            token = data.strip()
            self.dump[-1] += token
            word = token + "\u241E" + "\u241E".join(str_attrs) + "\u241E"
            self.current_part_tokens.append(word)

            token_lookup_data = dict(token_data)
            token_lookup_data.update(self.current_struct_annotations)
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
        else:
            whitespaces = data.splitlines(True)
            for ws in whitespaces:
                self.dump[-1] += ws
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


class StrixHTMLParser(HTMLParser):

    def __init__(self, strix_parser):
        self.strix_parser = strix_parser
        super(HTMLParser, self).__init__()
        HTMLParser.__init__(self)

    def handle_starttag(self, tag, attrs):
        self.strix_parser.handle_starttag(tag, dict(attrs))

    def handle_endtag(self, tag):
        self.strix_parser.handle_endtag(tag)

    def handle_data(self, data):
        self.strix_parser.handle_data(data)
