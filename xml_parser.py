# coding=utf-8
import os
import re
import xml.etree.cElementTree as etree
import logging

os.environ["PYTHONIOENCODING"] = "utf_8"

logger = logging.getLogger(__name__)


namespaces = {'xml': 'http://www.w3.org/XML/1998/namespace'}


def xml_to_json(xml_root, parse_as_list=[], parse_as_sublist=[], parse_as_bool=[], parse_as_inner_xml=[], nodename_translate={}, ignore=[]):
    """
    parse_as_list: multiple elems with same tagname on root level
    parse_as_bool: self explanatory
    parse_as_sublist: list of elements containing html under common parent, e.g
        <sources>
          <source>html <i>content</i></source>
        </sources>
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
        if child.tag in ignore: continue
        val = None
        if child.tag in parse_as_list:
            l = nodemap.get(nodename_translate.get(child.tag, child.tag), [])
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
        elif child.tag in parse_as_bool:
            val = child.text == "true"
        elif child.tag in parse_as_inner_xml:
            if len(child) or (child.text or "").strip(): # only if tag is not empty
                val = etree.tostring(child, encoding="unicode").strip()
            else:
                val = None
        else:
            val = child.text

        key = nodename_translate.get(child.tag, child.tag)
        nodemap[key] = val

    return nodemap


def parse_pipeline_xml(file_name, split_document, word_annotations, pre_data={}, trim_whitespace=False,
                       token_count_id=False, generate_markup=False):
    """
    split_document: everything under this node will go into separate documents
    word_annotations: a map of tag names and the attributes of those tags that
              will be included in annotation on word
    pre_data: if each segment (from split_document param) needs extra data,
              the split_document-tag currently needs an attribute called "id"
    """
    book_iter = etree.iterparse(file_name, events=("end",))

    current_part_tokens = []
    current_word_annotations = {}

    current_markup = []
    token_count = 0
    for event, element in book_iter:
        if event == "end":
            if element.tag == 'w':
                temp_word_annotations = dict(current_word_annotations)
                for annotation in word_annotations.get(element.tag, []):
                    temp_word_annotations[annotation] = annotation + "=" + element.get(annotation)

                if token_count_id:
                    temp_word_annotations['wid'] = 'wid=' + str(token_count)

                word = element.text + "|" + "|".join(temp_word_annotations.values()) + "||"
                current_part_tokens.append(word)

                if generate_markup:
                    current_markup.append('<span wid="' + str(token_count) + '">' + element.text + '</span>')

                token_count += 1

            elif element.tag in word_annotations:
                annotations = word_annotations[element.tag]
                for annotation in annotations:
                    current_word_annotations[element.tag + "_" + annotation] = annotation + "=" + element.get(annotation)

            elif element.tag == split_document:
                part_id = element.attrib.get("id")
                current_part = pre_data.get(part_id) or {}
                for attribute in element.attrib:
                    current_part[attribute] = element.attrib[attribute]
                if generate_markup:
                    current_part['markup'] = "".join(current_markup)
                current_part['text'] = " ".join(current_part_tokens)
                yield current_part
                token_count = 0
                current_part_tokens = []
                current_markup = []

            if generate_markup and (not trim_whitespace) and element.tail:
                current_markup.append(element.tail)

