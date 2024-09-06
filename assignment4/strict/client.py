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
  client_idx = int(sys.argv[1]) #passed in creation of process, unique value to each client process
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

  for i in range(num_requests):
    time.sleep(random.uniform(.05, .5)) #ARBITRARY DELAY TO TEST CONSISTENCY!
    prompt = [random.choice(['get', 'set']), random.choice(['a', 'b', 'c', 'd', 'e']), random.choice(list(range(100)))]
    msg = Message(0, 0, 'cli', prompt)
    send_message(random.choice(replica_ports), msg)    

if __name__ == '__main__': main()
