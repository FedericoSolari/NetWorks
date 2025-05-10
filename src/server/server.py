import logging
import os
from queue import Queue
from socket import socket, AF_INET, SOCK_DGRAM, timeout
from threading import Thread
from lib.constants import BUFFER_SIZE, TIMEOUT, ERROR_EXISTING_FILE
from threading import Lock
from lib.message_handling import Message
from lib.flags import START_SESSION, ACK, LIST
from lib.commands import Command
from lib.common_functions import get_file_name 
from lib.exceptions import TimeoutsRetriesExceeded
from lib.common_functions import send_error
from lib.protocol_fabric import select_protocol

class Server:
    def __init__(self, ip, port, args):
        if args.host is None:
            self.ip = ip
        else:
            self.ip = args.host
        if args.port is None:
            self.port = port
        else:
            self.port = args.port
        #self.ip = ip
        #self.port = port
        self.clients = {}
        self.protocols = {}
        self.protocols_lock = Lock()
        storage = args.storage
        self.storage = storage if storage is not None else "saved-files"

        if not os.path.isdir(self.storage): # si no existe el dir lo creo
            os.makedirs(self.storage, exist_ok=True)

    def start(self):
        self.socket = socket(AF_INET, SOCK_DGRAM)
        self.socket.bind((self.ip, self.port))

        print(f"Server {self.ip} is running on port {self.port}")
        try:
            self.handle_socket_messages()
        except Exception as e:
            logging.error(f"Error in server: {e}")
            raise e
    
    def handle_socket_messages(self):
        while True:
            encoded_message, client_address = self.socket.recvfrom(BUFFER_SIZE)
            client_port = client_address[1]
            try:
                client_msg_queue = self.clients[client_port]
                client_msg_queue.put(encoded_message)

            except KeyError:  # el cliente no esta en clientes
                client_msg_queue = Queue()
                client_msg_queue.put(encoded_message)
                self.clients[client_port] = client_msg_queue
                args = (encoded_message, client_address, client_msg_queue)
                try:
                    client = Thread(target=self.handle_client_message,
                                    args=args)
                    client.start()
                except Exception as e:
                    logging.error(f"Error in thread {e}")

    # Se completa el 3 way handshake: le masnda el ACK al cliente y espera q
    # el cliente le conteste otro ACK
    
    def send_and_wait_ack_from_client(self, client_address, msg_queue, decoded_msg):
        client_port = client_address[1]
        protocol_RDT = decoded_msg.data.decode()

        transfer_socket = socket(AF_INET, SOCK_DGRAM)
        #transfer_socket = self.socket
        protocol = select_protocol(protocol_RDT)
  
        self.protocols_lock.acquire()
        self.protocols[client_port] = protocol(transfer_socket)
        self.protocols_lock.release()
       
        print("envio start session ack")
        print(f" en el server :flecha_en_curva_a_la_derecha: Destino: IP={client_address[0]}, Port={client_address[1]}")
        print(f" en el server :flecha_en_curva_a_la_derecha: Socket fileno: {transfer_socket.fileno()}")
        print(f" en el servidor + {transfer_socket.getsockname()}")
            
        start_session_ack = Message.start_session_ack_msg(decoded_msg.command)
        transfer_socket.sendto(start_session_ack, client_address)
        transfer_socket.sendto(start_session_ack, client_address)
        transfer_socket.sendto(start_session_ack, client_address)

        try:
            encoded_message = transfer_socket.recvfrom(BUFFER_SIZE)[0]
            decoded_msg = Message.decode(encoded_message)
            print(f"Mensaje recibido del cliente: {decoded_msg}")
            print(f"Flag recibido: {decoded_msg.flags} ({type(decoded_msg.flags)})")
            #Server espera un ACK del cliente acrca del SEND_ACK que envio
            if decoded_msg.flags == ACK:
                self.start_file_transfer_operation(msg_queue,decoded_msg, client_address, transfer_socket)
                print("fin operacion de file transfer") 
            else:
                logging.error("Flag inesperado, cerrando conexión")
                self.close_client_connection(client_address)
        except Exception as e:
            del self.clients[client_port]
            logging.error(f"Client {client_port}: {e}")
            logging.error(
                f"Client {client_port}: handshake timeout." +
                " Closing connection."
            )
            raise e

    def handle_client_message(self, encoded_msg, client_address, msg_queue):
        try:
            encoded_msg = msg_queue.get(block=True, timeout=TIMEOUT)
            decoded_msg = Message.decode(encoded_msg)

            if decoded_msg.flags == START_SESSION:
                self.send_and_wait_ack_from_client(client_address, msg_queue, decoded_msg)
            else:
                logging.error(f"Cliente {client_address} envió un mensaje no válido para iniciar sesión.")
                self.close_client_connection(client_address)

        except timeout:
            logging.warning(f"No se recibió ACK de {client_address}, handshake incompleto")

        except Exception as e:
            logging.error(f"Error handling client message: {e}")
            raise e
        
    def handle_upload(self, client_address, client_msg_queue, transfer_socket):
        client_port = client_address[1]
        self.protocols_lock.acquire()
        protocol = self.protocols[client_port]
        self.protocols_lock.release()
        msg = transfer_socket.recvfrom(BUFFER_SIZE)[0]
        print(f"Primer mensaje recibido en upload: {Message.decode(msg)}")
        file_name = get_file_name(self.storage, Message.decode(msg).file_name)
        print(f"Uploading file to: {file_name}")
        try:
            protocol.receive_file(first_encoded_msg=msg,
                      client_port=client_port,
                      file_path=file_name,
                      server_address=client_address)

            print(f"File {file_name} uploaded, closing connection")
        except timeout:
            logging.error("Timeout on client")
            self.close_client_connection(client_address)

    def start_file_transfer_operation(self, client_msg_queue, decoded_msg, client_address, transfer_socket):
        client_port = client_address[1]
        print(f"Client {client_port}: iniciando la transferencia de archivos...")
        self.clients[client_port] = client_msg_queue
        if decoded_msg.command == Command.DOWNLOAD:
            print(f"Client {client_port}: iniciando download...")
            self.handle_download(client_address, client_msg_queue,transfer_socket)
        elif decoded_msg.command == Command.UPLOAD:
            print(f"Client {client_port}: iniciando la transferencia de archivos...")   
            self.handle_upload(client_address, client_msg_queue,transfer_socket)
            print(f"Client {client_port}: transferencia de archivos finalizada")
        else:
            logging.error(f"Client {client_port}: comando no soportado")
            self.close_client_connection(client_port)
    
    def handle_download(self, client_address, msg_queue, transfer_socket):
        print(f"handle_download {client_address}")
        client_port = client_address[1]
        encoded_client_message = transfer_socket.recvfrom(BUFFER_SIZE)[0]
        decoded_message = Message.decode(encoded_client_message)
        command = decoded_message.command

        self.protocols_lock.acquire()
        protocol = self.protocols[client_port]
        self.protocols_lock.release()

        if decoded_message.flags == LIST.encoded:
            print(f"Client {client_address}: enviando lista de archivos")
            self.send_file_list(client_address)
        else:
            print(f"Client {client_address}: iniciando download...")
            file_path = os.path.join(self.storage, decoded_message.file_name)
            if not os.path.exists(file_path):
                print(f"File {decoded_message.file_name} doesn't exist")
                send_error(transfer_socket, command, client_port,
                           ERROR_EXISTING_FILE)
                logging.error(f"File {decoded_message.file_name} doesn't exist, try again")
                return

            try:
                print(f"Client {client_address}: enviando archivo {decoded_message.file_name}")
                protocol.send_file(ip=client_address[0],client_port=client_port,
                                   file_path=file_path)
                self.close_client_connection(client_address)
            except TimeoutsRetriesExceeded:
                logging.error("Timeouts retries exceeded")
                self.close_client_connection(client_address)




    def send_file_list(self, client_address):
        files = os.listdir(self.storage)
        print("Server available files:")
        print(files)
        self.close_client_connection(client_address)
           
    
    def close_client_connection(self, client_address):
        self.clients.pop(client_address[1], None)
        print(f"Client {client_address}:")
        self.protocols_lock.acquire()
        self.protocols.pop(client_address[1], None)
        self.protocols_lock.release()
        
        print(f"Client {client_address[1]}: cerrando la conexion con el cliente...")

