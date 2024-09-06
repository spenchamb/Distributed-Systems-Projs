import json
import sys
import redis
import pickle
import time
import socket
from lib import *
import random

CONFIG_PATH = 'config.json'

def main():
  client_idx = int(sys.argv[1])

  filesystem = None
  try:
    with open(CONFIG_PATH, 'r') as file:
      filesystem = json.load(file)
  except FileNotFoundError:
    print('Error: Config file config.json is required in main path.')
    sys.exit(0)

  replica_ports = list(range(filesystem['replica_start_port'], filesystem['replica_start_port'] + filesystem['num_replicas']))
  client_ports = list(range(filesystem['client_start_port'], filesystem['client_start_port'] + filesystem['num_clients']))
  port = client_ports[client_idx]
  num_requests = filesystem['num_requests'] 

  #make socket for potential blocking scenarios
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  sock.bind(('localhost', port))
  sock.listen(100)

  for i in range(num_requests):
    prompt = [random.choice(['get', 'set']), random.choice(['a', 'b', 'c']), random.choice(list(range(100)))]
    msg = Message('CLIENT', prompt, port)
    if prompt[0] == 'get': send_message(random.choice(replica_ports), msg)
    else: #set operation, so BLOCKING!
      send_message(random.choice(replica_ports), msg)
      rect = None
      while rect != 'OK': #while OK message has not been received from replica (THIS IS BLOCKING)
        client_socket, client_address = sock.accept()
        rect = pickle.loads(client_socket.recv(1024))


if __name__ == '__main__': main()
