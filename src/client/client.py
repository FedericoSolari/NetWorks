import logging
import socket
from lib.flags import START_SESSION_ACK
from lib.constants import BUFFER_SIZE, TIMEOUT, MAX_TIMEOUT_RETRIES
from lib.message_handling import Message
from lib.commands import Command
from lib.protocol_fabric import select_protocol

class Client:  
    def __init__(self, ip, port, protocol):
        self.ip = ip
        self.port = port
        self.server_address = None 
        self.protocol = select_protocol(protocol)

    def start(self, command, action):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.settimeout(TIMEOUT)
        self.protocol = self.protocol(self.socket)

        maybe_ack = None
        tries = 0

        while tries < MAX_TIMEOUT_RETRIES:
            try:
                # 1) Envío START_SESSION
                print("Enviando START_SESSION al servidor")
                self.send_start_to_server(command, self.protocol)

                # 2) Espero START_SESSION_ACK
                data, addr = self.socket.recvfrom(BUFFER_SIZE)
                self.server_address = addr
                maybe_ack = Message.decode(data)

                if maybe_ack.flags == START_SESSION_ACK:
                    print("Recibido START_SESSION_ACK")
                    break
                else:
                    print(f"Recibido flag inesperado: {maybe_ack.flags}")
                    # podemos considerar esto un fallo de handshake
                    maybe_ack = None
                    tries += 1

            except socket.timeout:
                logging.error("Timeout esperando START_SESSION_ACK, retrying...")
                tries += 1

        # 3) Si nunca recibimos el ACK, abortamos
        if maybe_ack is None:
            logging.error("Handshake fallido: servidor no disponible.")
            return

        # 4) Mandamos el ACK de vuelta por cortesía (si tu protocolo lo requiere)
        self.send(Message.ack_msg(command, ack_num=0), self.server_address)
        print("Conectado al servidor, arrancamos acción.")
        action()

    def send_start_to_server(self, command, protocol):
        print("Enviando mensaje de inicio de sesion al servidor")
        start_session_msg = Message.start_session_msg(command, protocol)
        self.send(start_session_msg)
        print("Sent START SESSION to server")

    def send(self, message, address=None):
        if address:
            self.socket.sendto(message, address) 
        else:
            self.socket.sendto(message, (self.ip, self.port)) #entra siempre en el else

    def receive(self):
        return self.socket.recvfrom(BUFFER_SIZE)
