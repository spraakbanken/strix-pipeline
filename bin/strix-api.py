# -*- coding: utf-8 -*-
import sys
from strix.api.web import app
import strix.loghelper
from gevent.pywsgi import WSGIServer
from gevent import monkey
monkey.patch_all()

try:
    port = int(sys.argv[1])
except (IndexError, ValueError):
    sys.exit("Usage %s <port>" % sys.argv[0])

strix.loghelper.setup_file_logging()

WSGIServer(('0.0.0.0', port), app).serve_forever()
