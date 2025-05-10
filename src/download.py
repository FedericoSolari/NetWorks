import socket
from lib.commands import Command
from lib.constants import DOWNLOADS_DIR, MAX_TIMEOUT_RETRIES, LOG_LEVEL
from lib.exceptions import ServerConnectionError
from lib.message_handling import Message
from client.client import Client
from lib.argument_parser import parse_args_download
from lib.flags import ERROR, LIST
import sys
import logging
import os
from lib.common_functions import get_file_name 
from time import sleep
import time


def download(client, args):
    if args.files:
        print("Server will show files...")
        show_server_files(client)
        sys.exit(0)

    try:
        if not os.path.isdir(DOWNLOADS_DIR):
            os.makedirs(DOWNLOADS_DIR, exist_ok=True)

        download_using_protocol(client, args)
    except ServerConnectionError:
        logging.error("Server is offline")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        sys.exit(1)


def show_server_files(client):
    msg_to_send = Message(Command.DOWNLOAD, LIST, 0, "", b"")
    client.send(msg_to_send.encode()) #sin address
    msg_to_send = Message(Command.DOWNLOAD, LIST, 0, "", b"")
    client.send(msg_to_send.encode(), client.server_address)


def download_using_protocol(client, args):
    msg_to_send = Message.download_msg(args.name)

    try:
        encoded_messge, _ = send_with_retries(client, msg_to_send)
    except ServerConnectionError as e:
        logging.error(f"Error: {e}")
        raise

    decoded_msg = Message.decode(encoded_messge)
    if decoded_msg.flags == ERROR.encoded:
        print(decoded_msg.data)
        sys.exit(1)

    file_name = get_file_name(DOWNLOADS_DIR, args.dst)
    start_time = time.time()
    client.protocol.receive_file(first_encoded_msg=encoded_messge,
                                 file_path=file_name,
                                 server_address=client.server_address)
    end_time = time.time()
    duration = end_time - start_time
    print(f"Duración de la descarga: {duration:.2f} segundos")
    print("Download finished")

def send_with_retries(client, message, retries=MAX_TIMEOUT_RETRIES):
    # intento enviar un mensaje y recibir una respuesta, con reintentos ante Timeouts
    for attemp in range(retries):
        try:
            print(f"Attempt {attemp + 1} to send message a: {client.server_address}...")
            client.send(message, client.server_address)
            return client.receive()
        except socket.timeout:
            print(f"Attempt {attemp + 1} failed. Retrying...")
    logging.error("Max retries reached. Exiting.")
    raise ServerConnectionError("Max retries reached. Exiting.")

if __name__ == "__main__":
    try:
        sleep(2)
        args = parse_args_download()

        logging.basicConfig(level=LOG_LEVEL)

        #prepare a logging
        client = Client(args.host, args.port, args.RDTprotocol)
        client.start(Command.DOWNLOAD, lambda: download(client, args))
    except KeyboardInterrupt:
        print("\nExiting...")
        #action to send close message after download
        client.socket.send_to(Message.close_msg(Command.DOWNLOAD), (args.host, args.port))
        sys.exit(0)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        sys.exit(1)



