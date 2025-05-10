import sys
from lib.commands import Command
from client.client import Client
from lib.message_handling import Message
from protocol.stop_and_wait import StopAndWaitProtocol
from lib.argument_parser import parse_args_upload
import logging
from lib.constants import LOG_LEVEL
import time

def upload(client):
    try:
        start_time = time.time()
        client.protocol.send_file(args, server_address=client.server_address)
        end_time = time.time()
        duration = end_time - start_time
        print(f"File transfer duration: {duration:.2f} seconds")

    except KeyboardInterrupt:
        print("\nExiting...")
        client.protocol.socket.sendto(Message.close_ack_msg(Command.UPLOAD), client.client_address)
        sys.exit(0)


if __name__ == "__main__":
    try:
        print("Starting upload client...")
        args = parse_args_upload()

        logging.basicConfig(level=LOG_LEVEL) 

        print(args)

        client = Client(args.host, args.port, args.RDTprotocol)
        #client.socket.settimeout(args.timeout)
        client.start(Command.UPLOAD, lambda: upload(client))
        print("Client started successfully.")
    except KeyboardInterrupt:
        print("\nExiting...")
        sys.exit(0)
    except Exception as e:
        print(f"An error occurred. Server is not available. {e}")
        sys.exit(1)