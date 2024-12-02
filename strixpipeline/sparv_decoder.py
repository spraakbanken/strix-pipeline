import yaml
from yaml.loader import SafeLoader
import time

from strixpipeline.config import config


# create config files using the generated sparv config
def createConfig(data):
    corpusData = getConfig(data)
    if "title" not in corpusData.keys():
        corpusData["title"] = "n/a"
    if "document_id" not in corpusData.keys():
        corpusData["document_id"] = "generated"

    corpusData["updated_at"] = int(time.time())

    with open(config.settings_dir + "/corpora/" + data["corpus_id"] + ".yaml", "w") as file:
        yaml.dump(corpusData, file, sort_keys=False)

    return "Config files successfully created and updated"


# create config data
def getConfig(data):
    yearExist = False
    corpusTemplate = {}
    corpusTemplate["analyze_config"] = {}
    textList = []
    textListX = []
    tempList_struct = []
    for key, value in data.items():
        if key == "text_attributes":
            for x in value:
                for k1, v1 in x.items():
                    if k1 == "text:year":
                        yearExist = True
                    if type(v1) is str:
                        textList.append({k1.split(":")[1]: v1.replace("text_", "")})
                        textListX.append(k1.split(":")[1])
                    else:
                        textList.append({k1.split(":")[1]: replaceKey(v1, k1.split(":")[1])})
                        textListX.append(k1.split(":")[1])
        elif key == "struct_attributes":
            corpusTemplate["analyze_config"]["struct_attributes"] = {}
            with open(config.settings_dir + "/attributes/struct_elems.yaml") as file:
                struct_keys = yaml.safe_load(file).keys()

            text_xtra, struct_modified, textAttr = restructure(value, struct_keys)
            textListX.extend(textAttr)
            for key1, value1 in struct_modified.items():
                if key1 in struct_keys:
                    tempDict1 = []
                    for x in value1:
                        for k1, v1 in x.items():
                            if type(v1) is str:
                                tempDict1.append({k1: v1})
                            else:
                                tempDict1.append({k1: replaceKeyStruct(v1, k1)})
                    corpusTemplate["analyze_config"]["struct_attributes"][key1] = tempDict1
                else:
                    for x in value1:
                        for k1, v1 in x.items():
                            if type(v1) is str:
                                tempList_struct.append({k1: createDict(k1)})
                            else:
                                tempList_struct.append({k1: replaceKey(v1, k1)})
            corpusTemplate["text_tags"] = text_xtra
            corpusTemplate["text_tags"].append("text")
        elif key == "word_attributes":
            wordList = []
            wordList.append({"lemgram": "lemgram"})
            for x in value:
                for k1, v1 in x.items():
                    if k1 not in ["ufeats", "lex", "_tail", "_head"]:
                        if type(v1) is str:
                            wordList.append({k1: v1})
                        else:
                            wordList.append({k1: replaceKey(v1, k1)})
            corpusTemplate["analyze_config"]["word_attributes"] = wordList
        elif key == "mode":
            corpusTemplate["mode_id"] = value[0]["name"]
            corpusTemplate["mode_name"] = config.corpusconf.get_modes()[value[0]["name"]]["translation_name"]
            corpusTemplate["folderName"] = ""
        elif key == "text_annotation":
            corpusTemplate["split"] = value
        elif key == "corpus_name":
            if "swe" in value.keys() and ("eng" in value.keys()):
                corpusTemplate[key] = value
            elif "swe" in value.keys() and ("eng" not in value.keys()):
                value["eng"] = value["swe"]
                corpusTemplate[key] = value
            elif "eng" in value.keys() and ("swe" not in value.keys()):
                value["swe"] = value["eng"]
                corpusTemplate[key] = value
        elif key == "corpus_description":
            if "swe" in value.keys() and ("eng" in value.keys()):
                corpusTemplate[key] = value
            elif "swe" in value.keys() and ("eng" not in value.keys()):
                value["eng"] = value["swe"]
                corpusTemplate[key] = value
            elif "eng" in value.keys() and ("swe" not in value.keys()):
                value["swe"] = value["eng"]
                corpusTemplate[key] = value
        else:
            corpusTemplate[key] = value
    corpusTemplate["analyze_config"]["text_attributes"] = []
    corpusTemplate["analyze_config"]["text_attributes"].extend(textList)
    corpusTemplate["analyze_config"]["text_attributes"].extend(tempList_struct)
    if not yearExist:
        corpusTemplate["analyze_config"]["text_attributes"].append({"year": "year"})
    for xItem in textListX:
        if xItem == "_id":
            corpusTemplate["document_id"] = "_id"
        if xItem == "title":
            corpusTemplate["title"] = "title"
        elif xItem == "titel":
            corpusTemplate["title"] = "titel"
        elif "title" in xItem:
            corpusTemplate["title"] = xItem
        elif "name" in xItem:
            corpusTemplate["title"] = xItem

    return corpusTemplate


# create dict
def createDict(item):
    item_dict = {}
    item_dict["translation_name"] = {}
    item_dict["translation_name"]["swe"] = item.replace("_", " ").capitalize()
    item_dict["translation_name"]["eng"] = item.replace("_", " ").capitalize()
    item_dict["name"] = item
    return item_dict


# replace key
def replaceKey(item, item_name):
    itemX = {}
    for key, value in item.items():
        if key == "label" and type(value) is str or (key == "preset" and type(value) is str):
            itemX["translation_name"] = {}
            itemX["translation_name"]["swe"] = value.replace("_", " ").capitalize()
            itemX["translation_name"]["eng"] = value.replace("_", " ").capitalize()
        elif key == "label" and type(value) is dict or (key == "preset" and type(value) is dict):
            itemX["translation_name"] = {}
            itemX["translation_name"]["swe"] = value["swe"].capitalize()
            itemX["translation_name"]["eng"] = value["eng"].capitalize()
        else:
            itemX[key] = value
    itemX["name"] = item_name
    return itemX


# replace key
def replaceKeyStruct(item, item_name):
    return replaceKey(item, item_name.split("_")[1])


def restructure(data, struct_keys):
    reCreate = {}
    text_elements = []
    textAttr = []
    for item in data:
        for item_key, item_value in item.items():
            if ":" in item_key:
                if item_key.split(":")[0] in reCreate.keys():
                    reCreate[item_key.split(":")[0]].append({item_key.replace(":", "_"): item_value})
                else:
                    reCreate[item_key.split(":")[0]] = [{item_key.replace(":", "_"): item_value}]
                if item_key.split(":")[0] not in struct_keys:
                    text_elements.append(item_key.split(":")[0])
                    textAttr.append(item_key.replace(":", "_"))
            else:
                reCreate[item_key] = item_value

    return list(set(text_elements)), reCreate, list(set(textAttr))

def main(corpus_name):
    # Sparv config file that need to be decode into Strix config format
    with open(config.settings_dir + "/sparv2strix/" + corpus_name + ".yaml") as file:
        data = yaml.load(file, Loader=SafeLoader)

    createConfig(data)
