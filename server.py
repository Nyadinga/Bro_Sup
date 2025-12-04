import socket
import threading
import struct


port = 5050
address = socket.gethostbyname(socket.gethostname())

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((address, port))

server.listen()
print(f"Server listening on {address}:{port}")


def start_server():
    while True:
        conn, addr = server.accept()
        print(f"New connection from {addr}")

        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()

        print(f"Active connections: {threading.active_count() - 1}")


def handle_client(conn,addr):
    print(f"Handling connection from {addr}")
    connected = True
    HEADER_SIZE = 4

    while connected :
        try:
            msg_length_bytes = conn.recv(HEADER_SIZE)
            if not msg_length_bytes:
                break
            if len(msg_length_bytes) > HEADER_SIZE:
                print(f"Received partial header from {addr}, closing connection.")
                break
            msg_length = struct.unpack('>I', msg_length_bytes)[0]

            full_msg = b''
            bytes_received = 0
            while bytes_received < msg_length:
                chunk = conn.recv(min(msg_length - bytes_received, 2048))
                if not chunk:
                    break
                full_msg += chunk
                bytes_received += len(chunk)
        except Exception as e:
            print(f"Error handling client {addr}: {e}")
            break
    conn.close()
    print(f"Connection from {addr} closed.")
            
