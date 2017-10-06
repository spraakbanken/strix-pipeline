# -*- coding: utf-8 -*-
import sys
from strix.api.web import app
import strix.loghelper

try:
    port = sys.argv[1]
except IndexError:
    sys.exit("Usage %s <port>" % sys.argv[0])

strix.loghelper.setup_file_logging()

app.run(host="0.0.0.0", threaded=True, port=port)



