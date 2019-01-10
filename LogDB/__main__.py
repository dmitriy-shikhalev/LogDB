import json
import sys

from .server import Server


if len(sys.argv) != 2:
    print("Usage: python -m LogDB [config file name]")
else:
    config = json.load(open(sys.argv[1]))
    server = Server(config)
    server.run_forever()