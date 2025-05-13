import socket
import time
import os
import logging
from threading import Thread, Lock, Condition
from queue import Empty

from lib.commands import Command
from lib.constants import (
    BUFFER_SIZE, DATA_SIZE, LOCAL_HOST,
    WINDOW_RECEIVER_SIZE, MAX_WINDOW_SIZE,
    LOCAL_PORT, READ_MODE, WRITE_MODE, TIMEOUT, SELECTIVE_REPEAT
)
from lib.exceptions import FileSendingError, TimeoutsRetriesExceeded
from lib.file_controller import FileController
from lib.flags import ACK, CLOSE_ACK, CLOSE, NO_FLAGS, START_SESSION, START_SESSION_ACK
from lib.message_handling import send_ack, Message
from lib.common_functions import receive_from_queue_or_socket

class SelectiveRepeatProtocol:
    def __init__(self, sock: socket.socket):
        self.socket = sock
        self.name = SELECTIVE_REPEAT
        # Emisor
        self.send_base = 0
        self.next_seq_num = 0
        self.send_window_size = WINDOW_RECEIVER_SIZE
        self.sent_packets = {}           
        self.sent_packets_lock = Lock()
        self.unacked_count = 0
        self.unacked_lock = Lock()
        self.window_not_full = Condition(self.unacked_lock)
        self.acked_set = set()
        self.sending_complete = False
        # Receptor
        self.rcv_base = 0
        self.rcv_window_size = WINDOW_RECEIVER_SIZE
        self.rcv_buffer = []

    def listen_for_acks(self, command: Command, client_port: int, server_address=None):

        self.socket.settimeout(TIMEOUT)
        try:
            while True:
                with self.sent_packets_lock:
                    if self.sending_complete and not self.sent_packets:
                        break
                try:
                    data, addr = self.socket.recvfrom(BUFFER_SIZE)
                    msg = Message.decode(data)
                except socket.timeout:
                    now = time.time()
                    with self.sent_packets_lock:
                        for seq, (encoded, ts) in list(self.sent_packets.items()):
                            if now - ts >= TIMEOUT:
                                target = server_address or (LOCAL_HOST, client_port)
                                self.socket.sendto(encoded, target)
                                self.sent_packets[seq] = (encoded, now)
                                logging.debug(f"Retransmitting packet {seq} to {target}")
                    continue
                if msg.flags == ACK:
                    seq = msg.ack_number
                    logging.debug(f"Received ACK: {seq}")
                    with self.sent_packets_lock:
                        self.sent_packets.pop(seq, None)
                    self.acked_set.add(seq)
                    with self.unacked_lock:
                        if self.unacked_count > 0:
                            self.unacked_count -= 1
                        self.window_not_full.notify()
                    while self.send_base in self.acked_set:
                        self.acked_set.remove(self.send_base)
                        self.send_base += 1
                        logging.debug(f"Sliding send_base -> {self.send_base}")
        finally:
            self._close_emitter(command, client_port, server_address)

    def _close_emitter(self, command, client_port, server_address):
        tries = 0
        while tries < 5:
            with self.unacked_lock:
                if self.unacked_count > 0:
                    time.sleep(0.1)
                    tries += 1
                    continue
            target = server_address or (LOCAL_HOST, client_port)
            self.socket.sendto(Message.close_session_msg(command), target)
            maybe = receive_from_queue_or_socket(None, self.socket, TIMEOUT)
            if maybe:
                try:
                    if Message.decode(maybe).flags == CLOSE_ACK:
                        return
                except:
                    pass
            tries += 1
        logging.warning("No CLOSE_ACK recibido tras varios intentos")

    def send(self, command, port, data: bytes, file_controller, server_address=None):
        with self.window_not_full:
            while self.unacked_count >= self.send_window_size:
                self.window_not_full.wait()
            msg = Message(command, NO_FLAGS, len(data),
                          file_controller.file_name, data,
                          self.next_seq_num, 0)
            encoded = msg.encode()
            target = server_address or (LOCAL_HOST, port)
            self.socket.sendto(encoded, target)
            logging.debug(f"Sent packet {self.next_seq_num} to {target}")
            with self.sent_packets_lock:
                self.sent_packets[self.next_seq_num] = (encoded, time.time())
            self.unacked_count += 1
            self.next_seq_num += 1

    def send_file(self, args=None, message_queue=None,
                  client_port=LOCAL_PORT, file_path=None, server_address=None):
        if file_path:
            fc = FileController.from_file_name(file_path, READ_MODE)
            command = Command.DOWNLOAD
        else:
            fc = FileController.from_args(args.src, args.name, READ_MODE)
            command = Command.UPLOAD
        file_size = fc.get_file_size()
        total_pkts = (file_size // DATA_SIZE) + bool(file_size % DATA_SIZE)
        self.send_window_size = min(total_pkts, MAX_WINDOW_SIZE)
        logging.debug(f"Send window size: {self.send_window_size}")
        ack_thread = Thread(
            target=self.listen_for_acks,
            args=(command, client_port, server_address),
            daemon=True
        )
        ack_thread.start()
        try:
            while True:
                chunk = fc.read()
                if not chunk:
                    break
                self.send(command, client_port, chunk, fc, server_address)
            self.sending_complete = True
            with self.window_not_full:
                while self.unacked_count > 0:
                    self.window_not_full.wait(TIMEOUT)
            ack_thread.join()
        finally:
            fc.close()
            try:
                self.socket.close()
            except:
                pass

    def packet_is_within_window(self, seq):
        return self.rcv_base <= seq < self.rcv_base + self.rcv_window_size

    def _send_ack(self, command, seq_number, port, server_address=None):
        if server_address:
            send_ack(command, seq_number,
                     self.socket, server_address[1], server_address[0])
        else:
            send_ack(command, seq_number,
                     self.socket, port)

    def receive(self, decoded_msg, port, file_controller, server_address=None):
        seq = decoded_msg.seq_number
        if seq == self.rcv_base:
            self._deliver_in_order(decoded_msg, port, file_controller, server_address)
        elif self.packet_is_within_window(seq):
            self._buffer_out_of_order(decoded_msg, port, server_address)
        elif seq < self.rcv_base:
            self._send_ack(decoded_msg.command, seq, port, server_address)

    def _deliver_in_order(self, msg, port, fc, server_address):
        fc.write_file(msg.data)
        self.rcv_base += 1
        flushed = True
        while flushed:
            flushed = False
            for pkt in list(self.rcv_buffer):
                if pkt.seq_number == self.rcv_base:
                    self.rcv_buffer.remove(pkt)
                    fc.write_file(pkt.data)
                    self.rcv_base += 1
                    flushed = True
                    break
        self._send_ack(msg.command, msg.seq_number, port, server_address)

    def _buffer_out_of_order(self, msg, port, server_address):
        if msg.seq_number not in [p.seq_number for p in self.rcv_buffer]:
            self.rcv_buffer.append(msg)
        self._send_ack(msg.command, msg.seq_number, port, server_address)

    def receive_file(self, first_encoded_msg, file_path,
                     client_port=LOCAL_PORT, server_address=None):
        dest = os.path.dirname(file_path)
        if dest:
            os.makedirs(dest, exist_ok=True)
        output_path = file_path

        fc = FileController.from_file_name(output_path, WRITE_MODE)
        self.socket.settimeout(TIMEOUT)
        try:
            # Recibimos handshake o primer data
            if first_encoded_msg:
                raw, addr = first_encoded_msg if isinstance(first_encoded_msg, tuple) else (first_encoded_msg, None)
                msg0 = Message.decode(raw)
                # Si viene ACK de inicio (servidor en download), ignoramos y recibir primer data
                if msg0.flags in (START_SESSION_ACK, START_SESSION):
                    logging.debug("Handshake recibido, esperando primer paquete de datos...")
                    raw, addr = self.socket.recvfrom(BUFFER_SIZE)
                # Sino raw ya es primer data
            else:
                raw, addr = self.socket.recvfrom(BUFFER_SIZE)
            # Procesar data y CLOSE
            while True:
                msg = Message.decode(raw)
                if msg.flags == NO_FLAGS:
                    self.receive(msg, client_port, fc, server_address or addr)
                elif msg.flags == CLOSE:
                    ack = Message.close_ack_msg(msg.command)
                    tgt = server_address or addr or (LOCAL_HOST, client_port)
                    self.socket.sendto(ack, tgt)
                    break
                # siguiente paquete
                try:
                    raw, _ = self.socket.recvfrom(BUFFER_SIZE)
                except socket.timeout:
                    continue
            # Grace period para tardíos
            end = time.time() + TIMEOUT
            while time.time() < end:
                try:
                    raw, _ = self.socket.recvfrom(BUFFER_SIZE)
                except socket.timeout:
                    break
                late = Message.decode(raw)
                if late.flags == NO_FLAGS:
                    self.receive(late, client_port, fc, server_address or addr)
        except Exception as e:
            logging.error(f"[SelectiveRepeat] Error en receive_file: {e}")
        finally:
            fc.close()