from xml.etree import cElementTree as etree
import os, sys

os.environ["PYTHONIOENCODING"] = "utf_8"

try:
    saldom = etree.iterparse(open("saldom.xml", encoding="UTF-8"), events=("start", "end"))
except FileNotFoundError as e:
    print("saldom.xml not found. Download it from here: \n"
        "https://svn.spraakdata.gu.se/sb-arkiv/pub/lmf/saldom/saldom.xml")
    sys.exit(e)

outputFile = open("stems.txt", "w", encoding="UTF-8")

_, root = next(saldom)
for event, element in saldom:
    if event == "start":
        pass
    elif event == "end":
        if element.tag == "LexicalEntry":
            lemma = element \
                    .find("./Lemma/FormRepresentation/feat[@att='writtenForm']") \
                    .get("val")
            # we don't want multi-word lemgrams
            if " " in lemma: continue
            output = []
            for wordform in element.findall("WordForm/feat[@att='writtenForm']"):
                # also skip words with ampersands 
                # TODO should we skip these?
                if "-" in wordform.get("val"): continue
                output.append(wordform.get("val") + " => " + lemma + "\n")



            outputFile.writelines(sorted(set(output)))

    root.clear()

print("done.")