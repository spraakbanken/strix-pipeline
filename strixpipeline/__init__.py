import logging
import sys

# even if strix uses logging.DEBUG or logging.INFO
# we don't want to log everything from libs
for lib in ["elasticsearch", "elastic_transport", "urllib3", "sentence_transformers"]:
    logger = logging.getLogger(lib)
    logger.setLevel(logging.CRITICAL)

FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
ch = logging.StreamHandler(stream=sys.stdout)
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter(FORMAT))
logging.root.setLevel(logging.INFO)
logging.root.addHandler(ch)
