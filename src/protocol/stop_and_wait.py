import logging
from queue import Empty
import socket
from lib.commands import Command
from lib.file_controller import FileController
from lib.flags import CLOSE, CLOSE_ACK, NO_FLAGS, START_SESSION_ACK, START_SESSION
from lib.constants import BUFFER_SIZE, LOCAL_HOST, LOCAL_PORT, TIMEOUT
from lib.constants import MAX_TIMEOUT_RETRIES, WRITE_MODE
from lib.constants import READ_MODE, STOP_AND_WAIT
from lib.message_handling import Message
from lib.exceptions import DuplicatedACKError, TimeoutsRetriesExceeded

class StopAndWaitProtocol():
    def __init__(self, socket):
        self.socket = socket
        self.seq_num = 0
        self.ack_num = 0
        self.tries_send = 0
        self.name = STOP_AND_WAIT

    def receive(self, decoded_msg, ip ,port, file_controller,
                transfer_socket=None):
        #print(
        #    f"Receiving: {decoded_msg}" +
        #    f"next message expected: {self.ack_num}")
        if decoded_msg.flags == START_SESSION_ACK or decoded_msg.flags == START_SESSION:
            return

        if decoded_msg.seq_number == self.ack_num:
            file_controller.write_file(decoded_msg.data)
            # print(f"[SERVER] Escribiendo chunk: seq={decoded_msg.seq_number}, len={len(decoded_msg.data)}")
            self.ack_num += 1

        ack_msg = Message.ack_msg(decoded_msg.command, self.ack_num)
        if transfer_socket:
            # print(f"[SERVER] Enviando ACK con transfer_socket")
            # print(f"  :flecha_en_curva_a_la_derecha: Destino: IP={ip}, Port={port}")
            transfer_socket.sendto(ack_msg, (ip, port))
            transfer_socket.sendto(ack_msg, (ip, port))
            transfer_socket.sendto(ack_msg, (ip, port))
        else:
            # print(f"[SERVER] Enviando ACK con self.socket")
            # print(f"  :flecha_en_curva_a_la_derecha: Destino: IP={LOCAL_HOST}, Port={port}")
            self.socket.sendto(ack_msg, (LOCAL_HOST, port))
            self.socket.sendto(ack_msg, (LOCAL_HOST, port))
            self.socket.sendto(ack_msg, (LOCAL_HOST, port))

    
    def send(self, command, ip, port, data, file_controller, msg_queue=None, server_address=None):
        tries = 0
        msg = Message(command, NO_FLAGS, len(data), file_controller.file_name, data, self.seq_num, 0)
        while tries < MAX_TIMEOUT_RETRIES:
            try:
                # print(f"[CLIENT] Intento #{tries + 1} enviando paquete seq={self.seq_num}")
                if server_address:
                    #Print(f" caso server_address: Enviando paquete seq={self.seq_num} a {server_address[0]}:{server_address[1]}")
                    self.socket.sendto(msg.encode(), server_address)
                else:
                    #print(f" Enviando paquete seq={self.seq_num} a {ip}:{port}")
                    self.socket.sendto(msg.encode(), (ip, port))
                # Config del timeout 
                self.socket.settimeout(TIMEOUT)

                encoded_message = self.socket.recvfrom(BUFFER_SIZE)[0]
                ack_msg = Message.decode(encoded_message)

                if ack_msg.ack_number == self.seq_num + 1:
                    # print(f"[CLIENT] ACK recibido para seq={self.seq_num}.")
                    self.seq_num += 1
                    self.tries_send = 0  # Reiniciamos el contador de reintentos
                    return  # Salimos porque el ACK fue correcto
                # else:
                #     print(f"[CLIENT] ACK duplicado o inválido recibido: {ack_msg.ack_number}, esperando {self.seq_num + 1}")
            except (socket.timeout, Empty) as e:
                tries += 1
                # print(f"[CLIENT] Timeout esperando ACK, reintentando... intento {tries}/{MAX_TIMEOUT_RETRIES}")

        print(f"[CLIENT] Máximos intentos alcanzados ({MAX_TIMEOUT_RETRIES}), cerrando.")
        raise TimeoutsRetriesExceeded
    
    # def send(self, command, port, data, file_controller, msg_queue=None,
    #          server_address=None):
        
    #     if self.tries_send >= MAX_TIMEOUT_RETRIES:
    #         print("Max timeout retries reached")
    #         raise TimeoutsRetriesExceeded
    #     self.tries_send += 1
    #     msg = Message(command, NO_FLAGS, len(data),
    #                   file_controller.file_name, data, self.seq_num, 0)
    #     if server_address:
    #         self.socket.sendto(msg.encode(), server_address)
    #     else:
    #         self.socket.sendto(msg.encode(), (LOCAL_HOST, port))
    #     self.socket.settimeout(TIMEOUT)
    #     try:
            
    #         encoded_message = self.socket.recvfrom(BUFFER_SIZE)[0]
    #         # print(f'encoded_message : {encoded_message}')
            
    #         if Message.decode(encoded_message).ack_number <= self.seq_num:
    #             print(f"Client {port}: received duplicated ACK")
    #             raise DuplicatedACKError
    #         else:
    #             self.tries_send = 0
    #             self.seq_num += 1
    #     except (socket.timeout, Empty) as e:
    #         # print("Timeout receiving ACK message")
    #         print(f'Timeout receiving ACK message error : {e}')
    #         raise e
        
    def send_close_and_wait_for_response(self, socket_, msq_queue, client_port,
                            command, server_address=None):
        close_tries = 0
        while close_tries < MAX_TIMEOUT_RETRIES:
            try:               
                maybe_close_ack = None
                if msq_queue:
                    maybe_close_ack = msq_queue.get(block=True, timeout=TIMEOUT)
                else:
                    maybe_close_ack = socket_.recvfrom(BUFFER_SIZE)[0]


                if Message.decode(maybe_close_ack).flags == CLOSE_ACK.encoded:
                    print("Received close ACK")
                break
            except (socket.timeout, Empty):
                close_tries += 1    

    def send_file(self, args=None, msg_queue=None,ip=LOCAL_HOST,
                client_port=LOCAL_PORT, file_path=None, server_address=None):
        f_controller = None
        command = Command.UPLOAD

        # Abrir archivo a enviar
        if file_path:
            f_controller = FileController.from_file_name(file_path, READ_MODE)
            command = Command.DOWNLOAD
        else:
            f_controller = FileController.from_args(args.src, args.name, READ_MODE)

        data = f_controller.read()
        file_size = f_controller.get_file_size()

        # Enviar chunks
        while file_size > 0:
            data_length = len(data)
            try:
                # print(f'Hago send command: {command}, client_port: {client_port}, f_controller: {f_controller}, server_address: {server_address}')
                self.send(command, ip, client_port, data, f_controller,
                        server_address=server_address)
            except DuplicatedACKError:
                continue
            except (socket.timeout, Empty):
                print("Timeout! Retrying...")
                continue
            except TimeoutsRetriesExceeded:
                raise TimeoutsRetriesExceeded

            data = f_controller.read()
            file_size -= data_length

        # Enviar mensaje de cierre
        close_msg = Message.close_session_msg(command)
        if server_address:
            self.socket.sendto(close_msg, server_address)
        else:
            self.socket.sendto(close_msg, (LOCAL_HOST, client_port))

        print("[CLIENT] CLOSE enviado, esperando CLOSE_ACK...")

        # Esperar ACK de cierre
        self.send_close_and_wait_for_response(socket_=self.socket,
                                    msq_queue=msg_queue,
                                    client_port=client_port,
                                    command=command,
                                    server_address=server_address)

        print("[CLIENT] CLOSE_ACK recibido. Transferencia finalizada.")

        f_controller.close()

    # el server recibe un archivo mediante stop-and-wait. si recibe un CLOSE,
    # envía un CLOSE_ACK y cierra la conexión.
    def receive_file(self,
                     file_path,
                     client_port=LOCAL_HOST,      
                     first_encoded_msg=None,
                     server_address=None):
   
        f_controller = FileController.from_file_name(file_path, WRITE_MODE)
        self.socket.settimeout(TIMEOUT)

        try:
            if first_encoded_msg is not None:
                encoded_message = first_encoded_msg
            else:
                encoded_message = self.socket.recvfrom(BUFFER_SIZE)[0]

            while True:
                decoded_msg = Message.decode(encoded_message)

                if decoded_msg.flags == CLOSE:
                    print("[SERVER] Recibido CLOSE")
                    ack_msg = Message.close_ack_msg(decoded_msg.command)
                    if server_address:
                        self.socket.sendto(ack_msg, server_address)
                    else:
                        self.socket.sendto(ack_msg, (LOCAL_HOST, client_port))
                    print("[SERVER] Enviado CLOSE_ACK, cerrando archivo.")
                    break  

                peer_port = server_address[1] if server_address else client_port
                peer_ip = server_address[0] if server_address else LOCAL_HOST
                self.receive(
                    decoded_msg,
                    peer_ip,
                    peer_port,
                    f_controller,
                    transfer_socket=self.socket
                )

                encoded_message = self.socket.recvfrom(BUFFER_SIZE)[0]

        except socket.timeout as e:
            logging.error(f"Timeout esperando mensaje del cliente: {e}")

        finally:
            f_controller.close()
            print("[SERVER] Transferencia finalizada, archivo cerrado.")