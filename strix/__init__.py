import logging
import sys

# even if strix uses logging.DEBUG or logging.INFO
# we don't want to log everything from libs
el = logging.getLogger("elasticsearch")
el.setLevel(logging.WARN)
url = logging.getLogger("urllib3")
url.setLevel(logging.WARN)

FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
ch = logging.StreamHandler(stream=sys.stdout)
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter(FORMAT))
logging.root.setLevel(logging.INFO)
logging.root.addHandler(ch)
