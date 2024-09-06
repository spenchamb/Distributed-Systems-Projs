import socket
import sys
import string
import random

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
    

    for _ in range(5):
      rand_key = random.choice(string.ascii_uppercase)
      rand_rq = random.choice("GS")
      cmd = ""
      if rand_rq == "G":
        cmd = f"GET {rand_key}\r\n"
      else:
        rand_val = random.randint(0,9)
        cmd = f"SET {rand_key} 1\r\n{rand_val}\r\n"
      send_request(host, port, cmd)
    #send_request(host, port, "SET key1 1\r\na\r\n")
    #send_request(host, port, "SET key1 1\r\nb\r\n")
    #send_request(host, port, "SET key2 9\r\nvalue_two\r\n")
    #send_request(host, port, "GET key1\r\n")
