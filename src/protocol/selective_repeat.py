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
from lib.exceptions import WindowFullError, FileSendingError
import logging

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
        self.not_acknowledged = 0  # n° packets sent but not acknowledged yet
        self.not_acknowledged_lock = Lock()
        self.acks_map = {}
        self.thread_pool = {}
        self.acks_received = 0

    # corre un hilo separado que va a recibir y procesar los ACK 
    def listen_for_acks(self, msq_queue, client_port, command,server_address=None):
        
        tries = 0
        # es el timeout para que el recvfrom no este indefinidamnete 
        self.socket.settimeout(TIMEOUT)
        try:
            while self.acks_received <= self.max_sqn: #true si no se recibieron todos los ACKs
                try:
                    #voy a esperar a un mensaje desde el socket
                    data = self.socket.recvfrom(BUFFER_SIZE)[0]
                    msg = Message.decode(data)
                    
                    if msg.flags == ACK:
                        # es ACK => actualizo contenedores, elimino el thread que esperaba el ACK y anazo la ventana si corresponde
                        print(f"ACK recibido: {msg}")
                        self.process_ack_and_cleanup_thread(client_port,msg)
                    else:
                        print(f"Mensaje recibido no es un ACK: {msg}")
                        # print(f"flag: {msg.flags} y el comando: {ACK.encoded} NO ENCODED {ACK}")
                except (socket.timeout, Empty):
                    print("Timeout on main thread ack")
                    tries += 1
                    if tries >= MAX_ACK_RESEND_TRIES:
                        print("Max tries reached for main ACK thread")
                        break
                except Exception as e:
                    logging.error(f"Error receiving acks: {e}")
        finally:
            print("limpio los threads de ACK")
            for thread in self.thread_pool.values():
                thread.join()	
            print("Sending close msg")
            self.send_close_and_wait_for_response(msq_queue, client_port, command,server_address=server_address)

    def process_ack_and_cleanup_thread(self, client_port, message):
        ## recibo un mensaje y obtengo el ack number
        ack_number = message.ack_number
        print(f"Received ACK: {ack_number}")
        #suma el numero de acks recibidos 
        self.join_and_cleanup_ack_thread(message)
        self.update_unacked_count(-1)
        self.acks_received += 1
        #si el mensaje es de download debe mostrar en el LOG el mensaje recibido y el puerto del cliente
        if message.command == Command.DOWNLOAD:
            print(f"el mensaje recibido es: {message} y el puerto del cliente {client_port}")
        # si el ack recibido es el que corresponde a la base de envio, entonces muevo la ventana de envio
        # y si no, lo guardo en el buffer
        if ack_number == self.send_base:
            print(f"moviendo la ventana de envio, base de envio actual: {self.send_base}")
            self.move_send_window()
        else:
            print(f"Messy ACK recibido: {ack_number}")
            self.buffer.append(message)

    def move_send_window(self):
        self.buffer.sort()
        next_base = self.send_base + 1
        remaining_buffer = []

        for ack in self.buffer:
            if ack == next_base:
                next_base += 1
            else:
                remaining_buffer.append(ack)

        self.buffer = remaining_buffer
        self.send_base += min((next_base - self.send_base), self.window_size)

    def send_close_and_wait_for_response(self, msq_queue, client_port, command,server_address=None):
        close_tries = 0
        while close_tries < MAX_TIMEOUT_RETRIES:
            try:
                if server_address:  
                    self.socket.sendto(Message.close_session_msg(command), server_address)
                else:
                    self.socket.sendto(Message.close_session_msg(command),
                                        (LOCAL_HOST, client_port))
                maybe_close_ack = receive_from_queue_or_socket(msq_queue, self.socket, TIMEOUT)
                if Message.decode(maybe_close_ack).flags == CLOSE_ACK.encoded:
                    print("Received close ACK")
                break
            except (socket.timeout, Empty):
                close_tries += 1

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

        print(f"{self.acks_map}")
        print(f"{self.thread_pool}")

        while retries < max_retries:
            ack_queue = self.acks_map.get(ack_num)
            thread = self.thread_pool.get(ack_num)
            print(f"Joining thread for ACK: {ack_queue} {thread} ack_number {ack_num}")

            if ack_queue and thread:
                print(f"Joining thread for ACK: {ack_num}")
                
                try:
                    ack_queue.put_nowait(ack_num)  # Desbloquea el thread que espera el ACK
                except Exception as e:
                    logging.error(f"Error putting ack in queue: {e}")
                    break
                
                thread.join(timeout=2)

                # Limpieza
                del self.acks_map[ack_num]
                del self.thread_pool[ack_num]
                return
            else:
                sleep(0.1)  # Espera un poco antes de volver a intentar
                retries += 1

        print(f"Failed to join ACK thread for {ack_num} after {max_retries} retries")

    def send_file(self, args=None, msg_queue=None,  client_port=LOCAL_PORT, file_path=None, server_address=None):
        f_controller = None
        command = Command.UPLOAD
        if file_path:
            f_controller = FileController.from_file_name(file_path, READ_MODE)
            command = Command.DOWNLOAD
        else:
            f_controller = FileController.from_args(args.src, args.name, READ_MODE)


        file_size = f_controller.get_file_size()
        # caulculo la cantidad de paquetes que voy a neceitar, segun el DATA_SIZE 
        self.set_window_size(int(file_size / DATA_SIZE))

        # leo solo el priemr bloque, para asi mando eso
        data = f_controller.read()

        #lanzo el hilo que va a escuchar y manejar los ACK que me lleguen, entonces el hilo principal no queda bloqueado
        ack_thread = Thread(target=self.listen_for_acks,args=(msg_queue, client_port, command,server_address))
        ack_thread.start()

        try:
            while data:
                try:
                    self.send(command, client_port, data, f_controller,server_address=server_address)
                    data = f_controller.read()
                except WindowFullError:
                    continue
        finally:
            ack_thread.join(timeout=10)
            f_controller.close()
            
    def set_window_size(self, total_packet_count):
        optimal_window = self.compute_window_size(total_packet_count)
        self.window_size = optimal_window
        self.max_sequence_number = total_packet_count
        print(f"Window size: {self.window_size}")

    def calculate_window_size(self, total_packet_count):
        half_packets = total_packet_count // 2
        return min(half_packets, MAX_WINDOW_SIZE)

    def wait_for_ack(self, ack_number, ack_queue, encoded_msg, port,
                     server_address=None):
        # asumo que llega el ack numer esperado
        print(f"Wating for ack {ack_number}")
        succesfully_acked = False
        tries = 1
        while not succesfully_acked:
            try:
                ack_queue.get(block=True, timeout=TIMEOUT)
                print(f"[THREAD for ACK {ack_number}]" + "succesfully acked")
                succesfully_acked = True
            except Empty:
                if tries == MAX_ACK_RESEND_TRIES:
                    print(f"Max tries reached for ACK {ack_number}")
                    break
                else:
                    print(f"Timeout for ACK {ack_number}")
                    msg = Message.decode(encoded_msg)
                    try:
                        print(f"Sending msg back to server: {msg}")
                        if server_address:
                            self.socket.sendto(encoded_msg, server_address)
                        else:
                            self.socket.sendto(encoded_msg, (LOCAL_HOST, port))
                    except Exception as e:
                        logging.error("error al enviar mensaje devuelta al servidor ")
                    tries += 1
    
    def receive(self, decoded_msg, port, file_controller, server_address=None):
        print(f"Waiting for ack {self.rcv_base}")
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
            print(f"Window starts at {self.rcv_base}"
                          + f" & ends at {self.rcv_base + self.window_size-1}")
            print(f"Msg out of window: {decoded_msg.seq_number}")

    def process_expected_packet(self, decoded_msg, port, file_controller,
                                server_address=None):
        print("Received expected sqn")
        self.write_to_file(file_controller, decoded_msg)
        print(f"Received packet {decoded_msg} from port {port}")
        self.rcv_base += 1
        self.process_buffer(file_controller)
        sequence_number = decoded_msg.seq_number
        
        if server_address:
            print(f"Sending ACK_1: {sequence_number}")
            send_ack(decoded_msg.command, server_address[1],
                     sequence_number, self.socket)
        else:
            print(f"Sending ACK_2 : {sequence_number}")
            send_ack(decoded_msg.command, port, sequence_number, self.socket)
        self.seq_num += 1

    def already_acknowledged(self, decoded_msg):
        return decoded_msg.seq_number < self.rcv_base

    def send_duplicated_ack(self, decoded_msg, port, server_address=None):
        sequence_number = decoded_msg.seq_number
        print(f"Message was already acked: {sequence_number}")
        if server_address:
            send_ack(decoded_msg.command, server_address[1], sequence_number, self.socket)
        else:
            send_ack(decoded_msg.command, port, sequence_number, self.socket)

    def buffer_packet(self, decoded_msg, port, server_address=None):
        seq_num = decoded_msg.seq_number
        print(f"Received msg: {seq_num}")
        
        # si no existe el numero de secuencia en el buffer, lo agrego
        if self.ack_is_not_repeated(decoded_msg):
            self.buffer.append(decoded_msg)
            
        print(f"Sending ACK: {seq_num}")
        
        if server_address:
            send_ack(decoded_msg.command, server_address[1], seq_num, self.socket)
        else:
            send_ack(decoded_msg.command, port, seq_num, self.socket)

    def ack_is_not_repeated(self, decoded_msg):
        unique_sqns = [x.seq_number for x in self.buffer]
        print(f"Buffered seq nums {unique_sqns}")
        return decoded_msg.seq_number not in unique_sqns

    def process_buffer(self, file_controller):
        # escribo los paquetes consecutivo que tenga disponible n ele archvio y axtualizo el buffer
        if not self.buffer:
            print("Buffer is empty")
            return

        # ordeno los paquetes por numero de secuencia        
        sorted_packets = sorted(self.buffer, key=lambda pkt: pkt.seq_number)
        expected_seq_number = self.rcv_base + 1
        pending_packets  = []

        for pkt  in self.buffer:
            if pkt.seq_number == expected_seq_number :
                self.write_to_file(file_controller, pkt )
                expected_seq_number += 1
            else:
                pending_packets.append(pkt )

        self.buffer = pending_packets
        self.move_rcv_window(shift=expected_seq_number - self.rcv_base)

    def write_to_file(self, file_controller, packet):
        print(f"Writing to file sqn: {packet.seq_number}")
        file_controller.write_file(packet.data)

    def packet_is_within_window(self, message):
        # determino si le paquete que recibo esta en la ventana de recepcion
        window_end = self.rcv_base + self.window_size - 1

        seq = message.seq_number   
        if self.rcv_base <= seq <= window_end:      
            return True
        return False

    def send(self, command, port, data, file_controller, server_address=None):
        if self.window_is_not_full():
            message = Message(command,NO_FLAGS,len(data),file_controller.file_name,data,self.seq_num, 0,)
            if server_address:
                self.socket.sendto(message.encode(), server_address)
            else:
                self.socket.sendto(message.encode(), (LOCAL_HOST, port))
           
            self.spawn_packet_ack_thread(port, message,server_address)         
            self.seq_num += 1
            self.modify_not_acknowledged()
        else:
            raise WindowFullError

        if command == Command.UPLOAD:
            print(f'Sent msg: {message} to {self.seq_num} {file_controller.get_file_size()}')

    def window_is_not_full(self):
        return self.not_acknowledged < self.window_size
    
    def spawn_packet_ack_thread(self, client_port, message, server_addr=None):
        ack_queue = Queue()
        seq_number = message.seq_number

        # Preparar los argumentos para el hilo
        args = (seq_number, ack_queue, message.encode(), client_port, server_addr)
        self.acks_map[self.seq_num] = ack_queue
        
        # creo y lanzo el hilo que espera el ACK
        wait_ack_thread = Thread(target=self.wait_for_ack, args=args)
        wait_ack_thread.start()
        self.thread_pool[self.seq_num] = wait_ack_thread

    def modify_not_acknowledged(self):
        self.not_acknowledged_lock.acquire()
        self.not_acknowledged += 1
        self.not_acknowledged_lock.release()

    def send_file(self, args=None, message_queue=None,
                  client_port=LOCAL_PORT, file_path=None, server_address=None):
        file_controller = None
        client_command = Command.UPLOAD

        try:
            if file_path:
                file_controller = FileController.from_file_name(file_path, READ_MODE)
                client_command = Command.DOWNLOAD
            else:
                file_controller = FileController.from_args(args.src,
                                                        args.name, READ_MODE)
            
            file_size = file_controller.get_file_size()
            self.set_window_size(int(file_size / DATA_SIZE))
            
            data_chunk = file_controller.read()
            ack_listener_thread = Thread(target=self.listen_for_acks,
                                args=(message_queue, client_port, client_command,
                                    server_address))
            ack_listener_thread.start()

            while file_size > 0:
                data_length = len(data_chunk)
                try:
                    self.send(client_command, client_port, data_chunk, file_controller,
                            server_address=server_address)
                except WindowFullError:
                    continue
                data_chunk = file_controller.read()
                file_size -= data_length

            ack_listener_thread.join(timeout=10)
            
        except: FileSendingError

        finally: 
            if file_controller:
                file_controller.close()

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
        print(f"Window size: {self.window_size}")

    def calculate_window_size(self, number_of_packets):
        return min(int(number_of_packets / 2), MAX_WINDOW_SIZE)

    def wait_for_ack(self, number_ack, ack_queue, msg, port,
                     server_address=None):
        print(f"Wating for ack: {number_ack}")
        
        succesfully_acked = False
        # Numero de intentos iniciales
        tries = 1 

        # Si el ack no fue recibido, se vuelve a enviar el mensaje
        while not succesfully_acked:
            try:
                #espera que haya un elemento en la cola por max un tiempo de TIMEOUT
                # sino lanza una excepcion
                ack_queue.get(block=True, timeout=TIMEOUT) 
                print(f"[THREAD for ACK {number_ack}]" + "succesfully acked")
               
                # entonces si llega hasta aca es porque hay un elemento en la cola
                succesfully_acked = True
            except Empty:
                # si el numero de intentos es igual al maximo, entonces salgo del bucle
                if tries == MAX_ACK_RESEND_TRIES: 
                    print(f"Max tries reached for ACK {number_ack}")
                    break
                else:
                    # sino, vuelvo a enviar el mensaje
                    print(f"Timeout for ACK {number_ack}")
                    send_msg = Message.decode(msg)
                    try:
                        print(f"Sending msg back to server: {send_msg}")
                        if server_address:
                            self.socket.sendto(msg, server_address)
                        else:
                            self.socket.sendto(msg, (LOCAL_HOST, port))
                    except Exception as e:
                        logging.error(f"Error sending msg back to server: {e}")
                    tries += 1

    def receive_file(self, first_encoded_msg, file_path, client_port=LOCAL_PORT, server_address=None):
        f_controller = FileController.from_file_name(file_path, WRITE_MODE)
        self.socket.settimeout(TIMEOUT)

        encoded_message = first_encoded_msg
        decoded_message = Message.decode(encoded_message)

        # Mientras NO sea CLOSE, seguimos recibiendo y procesando paquetes
        while decoded_message.flags != CLOSE:
            self.receive(decoded_message, client_port, f_controller, server_address=server_address)
            encoded_message = self.socket.recvfrom(BUFFER_SIZE)[0]
            decoded_message = Message.decode(encoded_message)

        self.process_buffer(f_controller)
        # Llegó CLOSE: enviamos CLOSE_ACK y cerramos
        target = server_address if server_address else (LOCAL_HOST, client_port)
        self.socket.sendto(Message.close_ack_msg(decoded_message.command), target)

        f_controller.close()
        self.socket.settimeout(None)
