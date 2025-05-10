import sys
import os
from lib.constants import LOCAL_HOST, LOCAL_PORT, LOG_LEVEL
from server.server import Server
from lib.argument_parser import parse_args_server
import logging

if __name__ == "__main__":
    try:
        logging.basicConfig(level=LOG_LEVEL)

        args = parse_args_server()
        server = Server(LOCAL_HOST, LOCAL_PORT, args)
        #server = Server("10.0.0.1", 5001, args)
        server.start()
    except KeyboardInterrupt:
        print("\nExiting...")
        sys.exit(0)
