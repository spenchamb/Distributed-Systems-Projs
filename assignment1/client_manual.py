import socket
import sys

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

    cmd = ""
    while cmd != "end":
      print('enter command IN MEMCACHED SYNTAX (include newline characters!), or type \'end\' to end: ', end="")
      cmd = input()
      cmd = cmd.replace("\\r", "\r").replace("\\n", "\n")
      if cmd != "end": send_request(host, port, cmd)
