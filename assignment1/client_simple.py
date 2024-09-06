import socket
import sys
import re

def send_request(host, port, request):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((host, port))
        client_socket.sendall(request.encode('utf-8'))
        response = client_socket.recv(1024).decode('utf-8')
        if response[-1] == '\n': response = response[:-1]
        print(response)

if __name__ == "__main__":
    if len(sys.argv) != 3: sys.exit(1)
    host = sys.argv[1]
    
    try:
        port = int(sys.argv[2])
    except ValueError:
        sys.exit(1)

    cmds = ["GET key1\r\n",
            "SET key1 9\r\nvalue_one\r\n",
            "GET key1\r\n",
            "SET today 18\r\nJanuary 31st, 2024\r\n",
            "SET today 18\r\nFebruary 1st, 2024\r\n",
            "GET today\r\n",
            "GET yesterday\r\n",
            "SET yesterday 18\r\nJanuary 31st, 2024\r\n",
            "GET yesterday\r\n"]

    for cmd in cmds: send_request(host, port, cmd)
