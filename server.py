import socket
import threading


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

    while connected :
        try:
            msg_length_bytes = conn.recv(4)
            if not msg_length_bytes:
                break
            msg = conn.recv(1024).decode('utf-8')
            print(f"Received message from {addr}: {msg}")
            if msg == "DISCONNECT":
                connected = False
        except ConnectionResetError:
            connected = False
    conn.close()
    print(f"Connection from {addr} closed.")
            
