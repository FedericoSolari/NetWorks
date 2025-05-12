import socket
from time import sleep
from threading import Thread, Lock
from lib.commands import Command
from lib.constants import BUFFER_SIZE, DATA_SIZE, LOCAL_HOST
from lib.constants import MAX_TIMEOUT_RETRIES, TIMEOUT, WINDOW_RECEIVER_SIZE
from lib.constants import LOCAL_PORT, READ_MODE, SELECTIVE_REPEAT
from lib.constants import MAX_WINDOW_SIZE, MAX_ACK_RESEND_TRIES
from lib.constants import WRITE_MODE
from lib.exceptions import WindowFullError
from lib.file_controller import FileController
from lib.flags import ACK, CLOSE_ACK, CLOSE, NO_FLAGS
from lib.message_handling import send_ack
from lib.message_handling import Message
from lib.common_functions import receive_from_queue_or_socket
from queue import Empty, Queue
from lib.exceptions import WindowFullError, FileSendingError, TimeoutsRetriesExceeded
import logging
from socket import timeout
import time
from threading import Lock, Condition

# el WRITE_MODE es el modo que se abre el archivo para escribir, lo MISMO que en stop and wait
class SelectiveRepeatProtocol:
    def __init__(self, socket):
        self.socket = socket
        self.seq_num = 0
        self.name = SELECTIVE_REPEAT
        self.send_base = 0  # it is the first packet in the window == its sqn
        self.rcv_base = 0
        self.window_size = WINDOW_RECEIVER_SIZE
        self.buffer = []
        self.sent_packets = {}            # seq_num -> (encoded_msg, timestamp)
        self.sent_packets_lock = Lock()
        self.not_acknowledged = 0  # n° packets sent but not acknowledged yet
        self.not_acknowledged_lock = Lock()
        self.window_not_full = Condition(self.not_acknowledged_lock)
        self.acks_map = {}
        self.thread_pool = {}
        self.acks_received = 0

    """
    def listen_for_acks(self, msg_queue, client_port, command, server_address):
        self.socket.settimeout(TIMEOUT)
        try:
            while self.acks_received < self.max_sqn:
                try:
                    data = self.socket.recvfrom(BUFFER_SIZE)[0]
                    msg  = Message.decode(data)

                    if msg.flags == ACK:
                        seq = msg.ack_number

                        # 1) Quitar de los pendientes
                        with self.sent_packets_lock:
                            self.sent_packets.pop(seq, None)

                        # 2) Actualizar contadores y ventana
                        with self.not_acknowledged_lock:
                            self.not_acknowledged -= 1
                            self.window_not_full.notify_all()
                        self.acks_received += 1

                        if seq == self.send_base:
                            self.move_send_window()

                    # si viene otro tipo de mensaje, lo ignoramos o lo loggeamos
                except timeout:
                    # cada vez que haya timeout, recorremos pendientes y reenviamos los vencidos
                    now = time.time()
                    with self.sent_packets_lock:
                        for seq, (encoded, ts) in list(self.sent_packets.items()):
                            if now - ts > TIMEOUT:
                                target = server_address or (LOCAL_HOST, client_port)
                                self.socket.sendto(encoded, target)
                                # actualizar timestamp
                                self.sent_packets[seq] = (encoded, now)
        finally:
            # limpia recursos y cierra sesión
            self.send_close_and_wait_for_response(
                msg_queue, client_port, command, server_address
            )
        """

    def listen_for_acks(self, msg_queue, client_port, command, server_address):
        """
        Hilo que:
        - recibe ACKs y libera espacio en la ventana
        - tras cada TIMEOUT retransmite todos los paquetes pendientes
        - solo termina cuando no quede NINGÚN paquete pendiente en self.sent_packets
        """
        self.socket.settimeout(TIMEOUT)
        #print("[ACK-LISTENER] arrancando, pendientes iniciales =", len(self.sent_packets))

        try:
            # Mientras haya al menos un paquete pendiente de ACK:
            while True:
                with self.sent_packets_lock:
                    if not self.sent_packets:
                        break  # todos ACKed → salimos del bucle

                try:
                    data = self.socket.recvfrom(BUFFER_SIZE)[0]
                    msg  = Message.decode(data)

                    if msg.flags == ACK:
                        seq = msg.ack_number
                        with self.sent_packets_lock:
                            if seq in self.sent_packets:
                                # ACK nuevo: lo quitamos de pendientes
                                #print(f"[ACK-LISTENER] nuevo ACK seq={seq}")
                                self.sent_packets.pop(seq)
                                # liberar espacio en ventana
                                with self.not_acknowledged_lock:
                                    self.not_acknowledged -= 1
                                    self.window_not_full.notify_all()
                            

                except timeout:
                    # TIMEOUT → retransmitimos **todos** los que sigan en sent_packets
                    now = time.time()
                    with self.sent_packets_lock:
                        pendientes = list(self.sent_packets.keys())
                    #print(f"[ACK-LISTENER] timeout, retransmitiendo: {pendientes}")
                    with self.sent_packets_lock:
                        for seq, (encoded, ts) in list(self.sent_packets.items()):
                            if now - ts > TIMEOUT:
                                target = server_address or (LOCAL_HOST, client_port)
                                self.socket.sendto(encoded, target)
                                # actualizar timestamp para no retransmitir inmediatamente otra vez
                                self.sent_packets[seq] = (encoded, now)

        finally:
            #print("[ACK-LISTENER] todos los ACKs recibidos, cerrando sesión")
            # Cierra limpiamente la sesión (envía CLOSE y espera CLOSE_ACK)
            self.send_close_and_wait_for_response(msg_queue, client_port, command, server_address)



    def process_ack_and_cleanup_thread(self, client_port, message, server_address):
        ## recibo un mensaje y obtengo el ack number
        ack_number = message.ack_number
        logging.debug(f"Received ACK: {ack_number}")
        #suma el numero de acks recibidos 
        self.join_and_cleanup_ack_thread(message)
        self.update_unacked_count(-1)
        self.acks_received += 1
        #si el mensaje es de download debe mostrar en el LOG el mensaje recibido y el puerto del cliente
        if message.command == Command.DOWNLOAD:
            logging.debug(f"el mensaje recibido es: {message} , el ip dle cliente {server_address[0]} el puerto del cliente {server_address[1]}")
        # si el ack recibido es el que corresponde a la base de envio, entonces muevo la ventana de envio
        # y si no, lo guardo en el buffer
        if ack_number == self.send_base:
            logging.debug(f"moviendo la ventana de envio, base de envio actual: {self.send_base}")
            self.move_send_window()
        else:
            logging.debug(f"Messy ACK recibido: {ack_number}")
            self.buffer.append(message)
    """
    def send_close_and_wait_for_response(self, msq_queue, client_port, command,server_address):
        close_tries = 0
        while close_tries < MAX_TIMEOUT_RETRIES:
            try:
                if server_address:  
                    self.socket.sendto(Message.close_session_msg(command), server_address)
                else:
                    self.socket.sendto(Message.close_session_msg(command),
                                        (server_address[0], server_address[1]))
                maybe_close_ack = receive_from_queue_or_socket(msq_queue, self.socket, TIMEOUT)
                if Message.decode(maybe_close_ack).flags == CLOSE_ACK.encoded:
                    logging.debug("Received close ACK")
                break
            except (socket.timeout, Empty):
                close_tries += 1
    """
    def send_close_and_wait_for_response(self, msg_queue, client_port, command, server_address):
        """
        Envía CLOSE solo cuando no queden paquetes pendientes.
        Luego intenta leer CLOSE_ACK; si recv devuelve None (socket cerrado o timeout),
        rompe el bucle y deja que el finally superior cierre el socket.
        """
        close_tries = 0
        max_close_retries = 5
        print("[CLOSE] iniciando send_close_and_wait_for_response")

        while close_tries < max_close_retries:
            print(f"[CLOSE] intento {close_tries+1}/{max_close_retries}")
            # 1) Esperar a que todos los paquetes sean ACKed
            if self.not_acknowledged > 0:
                print(f"[CLOSE] aún pendientes={self.not_acknowledged}; durmiendo…")
                time.sleep(0.1)
                continue

            # 2) Enviar CLOSE
            if server_address:
                self.socket.sendto(Message.close_session_msg(command), server_address)
            else:
                self.socket.sendto(
                    Message.close_session_msg(command),
                    (server_address[0], server_address[1])
                )

            # 3) Leer CLOSE_ACK (protegido con receive_from_queue_or_socket)
            maybe_close_ack = receive_from_queue_or_socket(msg_queue, self.socket, TIMEOUT)
            print(f"[CLOSE] tal vez llegó CLOSE_ACK: {maybe_close_ack!r}")
            if not maybe_close_ack:
                print("[CLOSE] no CLOSE_ACK (None) → terminando cierre.")
                break

            try:
                if Message.decode(maybe_close_ack).flags == CLOSE_ACK.encoded:
                    print("[CLOSE] recibí CLOSE_ACK válido")
                    break
            except Exception:
                print("[CLOSE] IGNORO datos corruptos en CLOSE_ACK")
            
            close_tries += 1

        # No cerramos el socket aquí: lo hará el finally de send_file o receive_file.





    def update_unacked_count(self, unack_packet_count):
        # se obtieene el lock para modificar la cantidad de paquetes no reconocidos
        # si la cantidad es negativa y hay no reconocidos, se resta
        # si la cantidad es positiva, se suma
        self.not_acknowledged_lock.acquire()
        if (unack_packet_count < 0 and self.not_acknowledged > 0) or unack_packet_count > 0:
            self.not_acknowledged += unack_packet_count
        self.not_acknowledged_lock.release()

    # se encrga de cerrar correctamente el therad aosciado al ACK recibido
    def join_and_cleanup_ack_thread(self, msg_received):
        ack_num = msg_received.ack_number
        max_retries = 20
        retries = 0

        # logging.debug(f"map {self.acks_map}")
        # logging.debug(f"thread {self.thread_pool}")

        while retries < max_retries:
            ack_queue = self.acks_map.get(ack_num)
            thread = self.thread_pool.get(ack_num)
            # logging.debug(f"Try to Join thread for ACK: {ack_queue} {thread} ack_number {ack_num}")

            if ack_queue and thread:
                logging.debug(f"Joining thread for ACK: {ack_num}")
                
                try:
                    ack_queue.put_nowait(ack_num)  # Desbloquea el thread que espera el ACK
                except Exception as e:
                    logging.error(f"Error putting ack in queue: {e}")
                    break
                
                # thread.join(timeout=2)
                thread.join(timeout=10)

                # Limpieza
                del self.acks_map[ack_num]
                del self.thread_pool[ack_num]
                return
            else:
                sleep(0.3)  # Espera un poco antes de volver a intentar
                retries += 1

        logging.debug(f"Failed to join ACK thread for {ack_num} after {max_retries} retries")

    def wait_for_ack(self, ack_number, ack_queue, encoded_msg, port,server_address):
        # asumo que llega el ack numer esperado
        logging.debug(f"Wating for ack {ack_number}")
        succesfully_acked = False
        tries = 1
        while not succesfully_acked:
            try:
                #logging.debug(f"Tries: {tries} for ACK: {ack_number}")
                ack_queue.get(block=True, timeout=TIMEOUT)
                logging.debug(f"[THREAD for ACK {ack_number}]" + "succesfully acked")
                succesfully_acked = True
            except Empty:
                if tries == MAX_ACK_RESEND_TRIES:
                    logging.debug(f"Max tries reached for ACK {ack_number}")
                    break
                else:
                    #logging.debug(f"Timeout for ACK en wait_for_ack {ack_number}")
                    msg = Message.decode(encoded_msg)
                    try:
                        logging.debug(f"Sending msg back to server: {msg} , server_address: {server_address} , port: {port}")
                        if server_address:
                            self.socket.sendto(encoded_msg, server_address)
                        else:
                            self.socket.sendto(encoded_msg, (LOCAL_HOST, port))
                    except Exception as e:
                        logging.error("error al enviar mensaje devuelta al servidor ")
                    tries += 1

    def receive(self, decoded_msg, port, file_controller, server_address=None):
        # logging.debug(f"Waiting for ack {self.rcv_base}")
        if decoded_msg.seq_number == self.rcv_base:
            self.process_expected_packet(decoded_msg, port, file_controller,
                                            server_address=server_address)
        elif self.packet_is_within_window(decoded_msg):
            self.buffer_packet(decoded_msg, port,
                                server_address=server_address)
        elif self.already_acknowledged(decoded_msg):
            # client lost ack, send ack again
            self.send_duplicated_ack(decoded_msg, port,
                                        server_address=server_address)
        else:
            # otherwise it is not within the window and it is discarded
            logging.debug(f"Window starts at {self.rcv_base}"
                            + f" & ends at {self.rcv_base + self.window_size-1}")
            logging.debug(f"Msg out of window: {decoded_msg.seq_number}")

    def process_expected_packet(self, decoded_msg, port, file_controller, server_address=None):
        """
        Escribe el paquete en orden, reordena buffer y avanza rcv_base.
        """
        # 1) Escribir el paquete recibido
        file_controller.write_file(decoded_msg.data)
        logging.debug(f"Wrote packet {decoded_msg.seq_number}")

        # 2) Avanzar base en 1
        self.rcv_base += 1
        logging.debug(f"rcv_base -> {self.rcv_base}")

        # 3) Procesar out-of-order buffer
        delivered = True
        while delivered:
            delivered = False
            for pkt in list(self.buffer):
                if pkt.seq_number == self.rcv_base:
                    self.buffer.remove(pkt)
                    file_controller.write_file(pkt.data)
                    self.rcv_base += 1
                    delivered = True
                    logging.debug(f"Flushed buffered pkt {pkt.seq_number}, rcv_base -> {self.rcv_base}")
                    break

        # 4) Enviar ACK por último seq procesado
        ack_num = self.rcv_base - 1
        if server_address:
            send_ack(decoded_msg.command, ack_num, self.socket,
                        server_address[1], server_address[0])
        else:
            send_ack(decoded_msg.command, ack_num, self.socket, port)

    def already_acknowledged(self, decoded_msg):
        return decoded_msg.seq_number < self.rcv_base

    def send_duplicated_ack(self, decoded_msg, port, server_address=None):
        sequence_number = decoded_msg.seq_number
        # logging.debug(f"Message was already acked: {sequence_number}")
        if server_address:
            send_ack(decoded_msg.command, sequence_number, self.socket, server_address[1], server_address[0])
        else:
            send_ack(decoded_msg.command, sequence_number,self.socket,port)

    def buffer_packet(self, decoded_msg, port, server_address=None):
        seq_num = decoded_msg.seq_number
        logging.debug(f"Received msg: {seq_num}")
        
        # si no existe el numero de secuencia en el buffer, lo agrego
        if self.ack_is_not_repeated(decoded_msg):
            self.buffer.append(decoded_msg)
            
        logging.debug(f"Sending ACK: {seq_num}")
        
        if server_address:
            send_ack(decoded_msg.command, seq_num, self.socket, server_address[1], server_address[0])
        else:
            send_ack(decoded_msg.command, seq_num, self.socket, port)

    def ack_is_not_repeated(self, decoded_msg):
        unique_sqns = [x.seq_number for x in self.buffer]
        logging.debug(f"Buffered seq nums {unique_sqns}")
        return decoded_msg.seq_number not in unique_sqns

    def process_buffer(self, file_controller):
        print(f"[BUFFER] antes flush, pendientes: {[pkt.seq_number for pkt in self.buffer]}")
        if not self.buffer:
            return

            #1) Ordeno
        sorted_packets = sorted(self.buffer, key=lambda pkt: pkt.seq_number)

            #2) Escribo consecutivos
        expected_seq = self.rcv_base + 1
        pending = []
        for pkt in sorted_packets:
            if pkt.seq_number == expected_seq:
                self.write_to_file(file_controller, pkt)
                expected_seq += 1
            else:
                pending.append(pkt)

            #3) Actualizo buffer y base de recepción
        self.buffer = pending
            #En lugar de llamar a move_rcv_window, que suma otra vez a rcv_base
        self.rcv_base = expected_seq
        print(f"[BUFFER] después flush, nuevos rcv_base={self.rcv_base}, buffer left={[pkt.seq_number for pkt in self.buffer]}")

    def write_to_file(self, file_controller, packet):
        logging.debug(f"Writing to file sqn: {packet.seq_number}")
        file_controller.write_file(packet.data)

    def packet_is_within_window(self, message):
        # determino si le paquete que recibo esta en la ventana de recepcion
        window_end = self.rcv_base + self.window_size - 1

        seq = message.seq_number   
        if self.rcv_base <= seq <= window_end:      
            return True
        return False

    def send(self, command, port, data, file_controller, server_address=None):
        # 1) Esperar si la ventana está llena
        with self.window_not_full:
            while self.not_acknowledged >= self.window_size:
                self.window_not_full.wait()

            # 2) Construir y enviar
            message = Message(
                command, NO_FLAGS, len(data),
                file_controller.file_name, data,
                self.seq_num, 0
            )
            encoded = message.encode()
            target = server_address or (LOCAL_HOST, port)
            
            self.socket.sendto(encoded, target)

            # 3) Registrar para reenvíos por timeout
            with self.sent_packets_lock:
                self.sent_packets[self.seq_num] = (encoded, time.time())

            # 4) Actualizar contadores
            self.not_acknowledged += 1
            self.seq_num += 1

    def window_is_not_full(self):
        return self.not_acknowledged < self.window_size

    def spawn_packet_ack_thread(self, client_port, message, server_address):
        ack_queue = Queue()
        seq_number = message.seq_number

        # Preparar los argumentos para el hilo
        args = (seq_number, ack_queue, message.encode(), server_address[0], server_address)
        self.acks_map[self.seq_num] = ack_queue
        
        # creo y lanzo el hilo que espera el ACK
        wait_ack_thread = Thread(target=self.wait_for_ack, args=args)
        wait_ack_thread.start()
        self.thread_pool[self.seq_num] = wait_ack_thread

    def modify_not_acknowledged(self):
        self.not_acknowledged_lock.acquire()
        self.not_acknowledged += 1
        self.not_acknowledged_lock.release()
    """
    def send_file(self, args=None, message_queue=None, client_port=LOCAL_PORT, file_path=None, server_address=None):
        
        # 1) Inicializar FileController y determinar el comando
        if file_path:
            file_controller = FileController.from_file_name(file_path, READ_MODE)
            command = Command.DOWNLOAD
        else:
            file_controller = FileController.from_args(args.src, args.name, READ_MODE)
            command = Command.UPLOAD

        try:
            # 2) Calcular cuántos paquetes vamos a enviar y ajustar la ventana
            file_size = file_controller.get_file_size()
            total_packets = int(file_size / DATA_SIZE) + (1 if file_size % DATA_SIZE else 0)
            self.set_window_size(total_packets)

            # 3) Leer el primer bloque
            data = file_controller.read()

            # 4) Levantar el hilo que escucha ACKs (y reenvía expirados)
            ack_thread = Thread(
                target=self.listen_for_acks,
                args=(message_queue, client_port, command, server_address)
            )
            ack_thread.start()

            # 5) Enviar todos los bloques; send() internamente bloquea si la ventana está llena
            while data:
                self.send(command, client_port, data, file_controller, server_address)
                data = file_controller.read()

            # 6) Esperar a que el listener termine (y cierre la sesión)
            ack_thread.join()

        finally:
            # 7) Siempre cerramos el archivo
            file_controller.close()

            try:
                self.socket.close()
                logging.debug("SelectiveRepeatProtocol: transfer socket closed")
            except Exception:
                pass
    """
    def send_file(self, args=None, message_queue=None, client_port=LOCAL_PORT, file_path=None, server_address=None):
        if file_path:
            file_controller = FileController.from_file_name(file_path, READ_MODE)
            command = Command.DOWNLOAD
        else:
            file_controller = FileController.from_args(args.src, args.name, READ_MODE)
            command = Command.UPLOAD

        try:
            file_size = file_controller.get_file_size()
            total_packets = int(file_size / DATA_SIZE) + (1 if file_size % DATA_SIZE else 0)
            self.set_window_size(total_packets)

            data = file_controller.read()

            # 1) Levantar hilo de ACK
            ack_thread = Thread(
                target=self.listen_for_acks,
                args=(message_queue, client_port, command, server_address)
            )
            ack_thread.start()

            # 2) Enviar todos los paquetes
            while data:
                self.send(command, client_port, data, file_controller, server_address)
                data = file_controller.read()

            # 3) Esperar a que todos los paquetes sean reconocidos
            while self.not_acknowledged > 0:
                time.sleep(0.1)  # Espera breve para permitir ACKs

            # 4) Unir hilo de ACK para asegurar que finalice
            ack_thread.join(timeout=5)

        finally:
            file_controller.close()
            try:
                self.socket.close()
                logging.debug("SelectiveRepeatProtocol: transfer socket closed")
            except Exception:
                pass


    def move_rcv_window(self, shift):
        self.rcv_base += min(shift, WINDOW_RECEIVER_SIZE)

    def move_send_window(self):
        self.buffer.sort()
        expected_seq  = self.send_base + 1
        remaining_acks  = []

        for ack in self.buffer:
            if ack == expected_seq:
                expected_seq += 1 # Avanzamos si es el paquete esperado
            else:
                remaining_acks.append(ack)

        self.buffer = remaining_acks
        shift = expected_seq - self.send_base
        self.send_base += min(shift, self.window_size)

    def set_window_size(self, number_of_packets):
        self.window_size = self.calculate_window_size(number_of_packets)
        self.max_sqn = number_of_packets
        logging.debug(f"Window size: {self.window_size}")

    def calculate_window_size(self, number_of_packets):
        return min(number_of_packets, MAX_WINDOW_SIZE)


    def receive_file(self, first_encoded_msg, file_path,
                    client_port=LOCAL_PORT, server_address=None):
        """
        - Ignora handshake hasta el primer data.
        - Recibe y procesa todos los paquetes normales hasta CLOSE.
        - Tras CLOSE, abre un “grace period” de un TIMEOUT extra para datos in-flight.
        - Hace flush final del buffer out-of-order y envía CLOSE_ACK.
        """
        fc = FileController.from_file_name(file_path, WRITE_MODE)
        self.socket.settimeout(TIMEOUT)
        #print("[RECEIVER] iniciar receive_file")

        try:
            # 1) Ignorar handshake hasta primer paquete con NO_FLAGS
            enc = first_encoded_msg
            msg = Message.decode(enc)
            while msg.flags != NO_FLAGS:
                #print(f"[RECEIVER] ignorando flags={msg.flags}")
                if msg.flags == CLOSE:
                    return
                try:
                    enc = self.socket.recvfrom(BUFFER_SIZE)[0]
                    msg = Message.decode(enc)
                except socket.timeout:
                    continue

            # 2) Bucle principal de datos
            while msg.flags != CLOSE:
                #Print(f"[RECEIVER] data seq={msg.seq_number}")
                try:
                    self.receive(msg, client_port, fc, server_address=server_address)
                    enc = self.socket.recvfrom(BUFFER_SIZE)[0]
                    msg = Message.decode(enc)
                except socket.timeout:
                    continue

            # 3) Grace period tras CLOSE: damos un TIMEOUT extra
            #print("[RECEIVER] recibí CLOSE, iniciando grace period")
            end_wait = time.time() + TIMEOUT
            while time.time() < end_wait:
                try:
                    #print("[RECEIVER] grace period, esperando más data…")
                    enc = self.socket.recvfrom(BUFFER_SIZE)[0]
                    msg = Message.decode(enc)
                    if msg.flags == NO_FLAGS:
                        #print(f"[RECEIVER] grace period: llegada tardía seq={msg.seq_number}")
                        self.receive(msg, client_port, fc, server_address=server_address)
                except socket.timeout:
                    break

            # 4) Flush final del buffer out-of-order
            self.process_buffer(fc)

            # 5) Enviar CLOSE_ACK final
            target = server_address or (LOCAL_HOST, client_port)
            #print("[RECEIVER] enviando CLOSE_ACK final")
            self.socket.sendto(Message.close_ack_msg(msg.command), target)

        except TimeoutsRetriesExceeded:
            logging.info("SelectiveRepeat: timeouts excedidos recibiendo, cerrando sesión limpia")

        finally:
            fc.close()
            try:
                self.socket.close()
            except Exception:
                pass
