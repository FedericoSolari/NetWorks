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
        print("seteo skt")
        print("seteo skt timeout")
        self.protocol = self.protocol(self.socket)
        print("seteo protocol")

        start_session_tries = 0
        while start_session_tries < MAX_TIMEOUT_RETRIES:
            try:
                print("envio mensaje de start session") 
                self.send_start_to_server(command, self.protocol)
                self.send_start_to_server(command, self.protocol)
                self.send_start_to_server(command, self.protocol)
                print("espero recibir msj")
                
                enconded_message, server_address = self.socket.recvfrom(BUFFER_SIZE)
                print("recibi msj")

                self.server_address = server_address
                maybe_start_session_ack = Message.decode(enconded_message) 
                print("ACK exitoso con servidor")
                break

            except ValueError as e:
                logging.error(f"Error de value error: {e}")
            except TypeError as e:
                logging.error(f"Error de type error: {e}")
            except socket.timeout:
                logging.error("Timeout esperando al servidor START SESSION " +
                              "respuesta. Intentando nuevamente...")
                start_session_tries += 1

        if start_session_tries == MAX_TIMEOUT_RETRIES:
            logging.error("Tiempo de espera agotado para la respuesta de INICIAR SESION, se alcanzo el numero maximo de reintentos")
            
        if maybe_start_session_ack.flags == START_SESSION_ACK:  
            self.send(Message.ack_msg(command, ack_num=0), self.server_address)
            # self.send(Message.ack_msg(command, ack_num=0), self.server_address)
            # self.send(Message.ack_msg(command, ack_num=0), self.server_address)

            print("Conectado al servidor")
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
