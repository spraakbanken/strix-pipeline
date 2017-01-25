# -*- coding: utf-8 -*-
import sys
from strix.api.web import app

from waitress import serve

try:
    port = sys.argv[1]
except IndexError:
    sys.exit("Usage %s <port>" % sys.argv[0])


serve(app, host='0.0.0.0', port=port)


