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

    cmd, rq = " ", " "
    print('This is a simplified, interactive client for the server. Syntax is as below:')
    print('<GET/get> <key>')
    print('<SET/set> <key> <\'value\'> (SINGLE QUOTES REQUIRED!)')
    while cmd != "end":
      print('enter command, or type \'end\' to end: ', end="")
      cmd = input()
      words = cmd.split()
      if cmd.startswith("SET") or cmd.startswith("set"):
        rq = "SET {0} {1}\r\n{2}\r\n".format(words[1], len(words[2]), cmd[cmd.find("'") + 1: cmd.rfind("'")])
      elif cmd.startswith("GET") or cmd.startswith("get"):
        rq = "GET {}\r\n".format(words[1])
      if cmd != "end": send_request(host, port, rq)
