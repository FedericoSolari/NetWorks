import os
from lib.message_handling import Message
from lib.constants import LOCAL_HOST
from lib.constants import BUFFER_SIZE

def get_file_name(directory, file_name):
    base_name, extension = os.path.splitext(file_name)
    candidate = os.path.join(directory, file_name)
    counter = 1

    while os.path.exists(candidate):
        candidate = os.path.join(directory, f"{base_name}_{counter}{extension}")
        counter += 1

    return candidate

def send_error(socket, command, port, error_msg):
    # Envia un mensaje de error 
    encoded_msg = Message.error_msg(command, error_msg)
    socket.sendto(encoded_msg, (LOCAL_HOST, port))


def receive_from_queue_or_socket(queue, sock, timeout=None):
    # Recibe un mensaje desde una cola (modo servidor) o desde el socket (modo cliente).
    if queue:
        return queue.get(block=True, timeout=timeout)
    # Recibe directamente del socket (cliente)
    message, _ = sock.recvfrom(BUFFER_SIZE)
    return message

def send_udp_ack(command, ack_number, destination_addr, sock):
    # Envía un mensaje ACK por socket UDP.

    try:
        ack_msg = Message.ack_msg(command, ack_number)
        sock.sendto(ack_msg, destination_addr)
        print(f"Sent ACK {ack_number} to {destination_addr}")
    except Exception as e:
        print(f"Error sending ACK {ack_number} to {destination_addr}: {e}")



