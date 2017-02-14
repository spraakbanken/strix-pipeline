# coding=utf-8
import os
import re
import xml.etree.cElementTree as etree
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
                       struct_annotations=(),
                       token_count_id=False,
                       set_text_attributes=False,
                       process_token=lambda x: None):
    """
    split_document: everything under this node will go into separate documents
    word_annotations: a map of tag names and the attributes of those tags that
              will be included in annotation on word
    """

    book_iter = etree.iterparse(file_name, events=("start", "end"))

    current_part_tokens = []
    current_word_annotations = {}
    current_struct_annotations = {}

    current_token_lookup = []
    dump = [""]
    token_count = 0
    lines = [[0]]

    _, root = next(book_iter)
    if root.tag == split_document:
        part_attributes = {}
        for attribute in root.attrib:
            part_attributes[attribute] = root.attrib[attribute]
    for event, element in book_iter:
        if set_text_attributes and event == "start" and element.tag == split_document:
            part_attributes = {}
            for attribute in element.attrib:
                part_attributes[attribute] = element.attrib[attribute]
        if event == "end":
            if element.tag == "w":
                token_data = dict(current_word_annotations)
                for annotation in word_annotations.get("w", []):
                    annotation_name = annotation["name"]
                    if "nodeName" in annotation:
                        annotation_value = element.get(annotation["nodeName"])
                    else:
                        annotation_value = element.get(annotation_name)
                    if annotation["set"]:
                        annotation_value = list(filter(bool, annotation_value.split("|")))
                    token_data[annotation_name] = annotation_value

                if token_count_id:
                    token_data["wid"] = token_count

                struct_data = {}
                for tag_name, annotations in current_struct_annotations.items():
                    if "length" not in annotations:
                        annotations["length"] = 1
                    else:
                        offset = -annotations["length"]
                        start_token_data = current_token_lookup[offset]["attrs"]
                        start_token_data[tag_name]["length"] += 1
                        current_struct_annotations[tag_name]["start_wid"] = start_token_data["wid"]

                    for k, attr in annotations.items():
                        if k == "attrs":
                            for annotation_name, v in attr.items():
                                struct_data[tag_name + "_" + annotation_name] = v

                process_token(token_data)
                all_data = dict(token_data)
                all_data.update(struct_data)

                str_attrs = []
                for attr, v in sorted(all_data.items()):
                    if isinstance(v, list):
                        v = "\u241F" + "\u241F".join(v) + "\u241F" if len(v) > 0 else "\u241F"
                    str_attrs.append(attr + "=" + str(v))

                token = element.text.strip()
                dump[-1] += element.text.strip()
                word = token + "\u241E" + "\u241E".join(str_attrs) + "\u241E"
                current_part_tokens.append(word)

                token_lookup_data = dict(token_data)
                token_lookup_data.update(current_struct_annotations)
                current_token_lookup.append({"word": token, "attrs": token_lookup_data, "position": token_count})

                token_count += 1

            elif element.tag == split_document:
                if set_text_attributes:
                    current_part = part_attributes
                else:
                    current_part = {}
                current_part["token_lookup"] = current_token_lookup
                current_part["dump"] = dump
                current_part["lines"] = lines
                current_part["word_count"] = len(current_part_tokens)
                current_part["text"] = "\u241D".join(current_part_tokens)
                yield current_part

                token_count = 0
                current_part_tokens = []
                current_token_lookup = []
                current_word_annotations = {}
                current_struct_annotations = {}
                dump = [""]
                lines = [[0]]

            elif element.tag in struct_annotations:
                del current_struct_annotations[element.tag]

            if element.tail:
                whitespaces = element.tail.splitlines(True)
                for ws in whitespaces:
                    dump[-1] += ws
                    if ws[-1] == "\n":
                        current_token = token_count - 1
                        add_whitespace(dump, lines, current_token)

            root.clear()

        if event == "start":
            if element.tag in struct_annotations:
                current_struct_annotations[element.tag] = {"attrs": {}}
                annotations = struct_annotations[element.tag]

                for annotation in annotations:
                    annotation_name = annotation["name"]
                    if "nodeName" in annotation:
                        a_value = element.get(annotation["nodeName"])
                    else:
                        a_value = element.get(annotation_name)

                    current_struct_annotations[element.tag]["attrs"][annotation_name] = a_value

            if element.tag != "w" and element.tag in word_annotations:
                annotations = word_annotations[element.tag]

                for annotation in annotations:
                    annotation_name = annotation["name"]
                    if "nodeName" in annotation:
                        a_value = element.get(annotation["nodeName"])
                    else:
                        a_value = element.get(annotation_name)

                    current_word_annotations[annotation_name] = a_value


def add_whitespace(dump, lines, current_token):
    dump.append("")
    [begin] = lines[-1]
    if begin == current_token + 1:
        lines[-1] = [-1]
    else:
        lines[-1] = [begin, current_token]
    lines.append([current_token + 1])
