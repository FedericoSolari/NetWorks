import logging
from lib.constants import EMPTY_FILE, EMPTY_DATA, BUFFER_SIZE, LOCAL_PORT, LOCAL_HOST#, MAX_TIMEOUT_RETRIES, TIMEOUT
from lib.flags import START_SESSION, ACK, CLOSE, CLOSE_ACK, ERROR, LIST, START_SESSION_ACK, NO_FLAGS, Flag
from lib.constants import LOCAL_HOST
from lib.commands import Command

def add_padding(data: bytes, n: int):
    k = n - len(data)
    if k < 0:
        raise ValueError
    return data + b"\0" * k

class Message:
    def __init__(self, command, flags, data_length, file_name, data, seq_number=0, ack_number=0):
        self.command = command
        self.flags = flags
        self.data_length = data_length
        self.file_name = file_name
        self.seq_number = seq_number
        self.ack_number = ack_number
        self.data = data

    def __str__(self):
        return (
            f"Message: "
            f"command={self.command}, "
            f"flags={self.flags}, "
            f"data_length={self.data_length}, "
            f"file_name={self.file_name}, "
            f"seq_number={self.seq_number}, "
            f"ack_number={self.ack_number}, "
        )

    @classmethod
    def decode(cls, bytes_arr: bytes):
        try:
            command = Command.from_values(bytes_arr[0])
        except ValueError:
            print("Invalid command")
            raise ValueError("Invalid command")

        flags = Flag(bytes_arr[1])
        f_data = int.from_bytes(bytes_arr[2:6], byteorder="big")
        file_name_bytes = bytes_arr[6:406]
        f_name = file_name_bytes.decode().strip('\0')
        seq_n = int.from_bytes(bytes_arr[406:410], byteorder="big")
        ack_n = int.from_bytes(bytes_arr[410:414], byteorder="big")
        data = bytes_arr[414:414 + f_data]

        return Message(command, flags, f_data, f_name, data, seq_n, ack_n)

    def encode(self):
        bytes_arr = b""
        bytes_arr += self.command.get_bytes()
        bytes_arr += self.flags.get_bytes()
        bytes_arr += self.data_length.to_bytes(4,signed=False, byteorder='big')

        if self.file_name is not None:
            bytes_arr += add_padding(self.file_name.encode(), 400)

        bytes_arr += self.seq_number.to_bytes(4, signed=False, byteorder='big')
        bytes_arr += self.ack_number.to_bytes(4, signed=False, byteorder='big')
        bytes_arr += add_padding(self.data, BUFFER_SIZE - len(bytes_arr))

        return bytes_arr

    @classmethod
    def ack_msg(cls, command, ack_num):
        msg = Message(command, ACK, EMPTY_FILE, "", EMPTY_DATA, 0, ack_num)
        return msg.encode()

    @classmethod
    def close_session_msg(cls, command):
        return Message(command, CLOSE, EMPTY_FILE, "", EMPTY_DATA).encode()

    @classmethod
    def start_session_ack_msg(cls, command):
        return Message(command, START_SESSION_ACK, EMPTY_FILE, "", EMPTY_DATA).encode()

    @classmethod
    def start_session_msg(cls, command, protocol):
        return Message(command, START_SESSION, len(protocol.name.encode()), "",
                       protocol.name.encode()).encode()

    @classmethod
    def error_msg(cls, command, error_msg):
        msg = Message(command, ERROR, EMPTY_FILE, "", data=error_msg.encode())
        return msg.encode()  

    @classmethod
    def close_ack_msg(cls, command):
        return Message(command, CLOSE_ACK, EMPTY_FILE, "", EMPTY_DATA).encode()

    @classmethod
    def download_msg(cls, file_name):
        msg = Message(Command.DOWNLOAD, NO_FLAGS, EMPTY_FILE,
                      file_name, EMPTY_DATA)
        return msg.encode()

def send_ack(command, ack_number, socket, port=LOCAL_PORT, ip=LOCAL_HOST):
    try:
        ack_msg = Message.ack_msg(command, ack_number)
        socket.sendto(ack_msg, (ip, port))
        logging.debug(f"[SERVER] Enviando ACK {ack_number} a {ip}:{port}")
    except Exception as e:
        logging.error(f"Error sending ACK: {e}")