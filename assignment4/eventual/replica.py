import sys
import pickle
import json
import os, subprocess
import time
import redis
import atexit
import random
from lib import *

CONFIG_PATH = 'config.json'
PORT = None
REDIS_PORT = None
PROC = None
queue = []
sock = None
NUM_CLIENTS = 0
REPLICA_PORTS = []
SOCK_TIMEOUT = None
REDIS = None
PORT_IDX = 0

def exit_handler(): #exit behavior, just close anything that may be running
  global REDIS
  if REDIS: REDIS.shutdown()
  if sock: sock.close()
atexit.register(exit_handler)

def start_replica(port): #initialize Replica object
  print(f'Starting replica on port {port}...')
  replica = Replica(port)
  replica.sock.settimeout(SOCK_TIMEOUT)
  accept_connections(replica)

def accept_connections(replica): #accept client connection and handle timeout behavior
  while True:
    try:
      client_socket, client_address = replica.sock.accept()
      handle_connection(client_socket, replica)
    except socket.timeout:
      break
  print(f'{replica.port}: exiting...')
  replica.sock.close()
  sys.exit(0)

def handle_connection(client_socket, replica):
  time.sleep(random.uniform(.02, .1)) #Arbitrary delay input for testing purposes
  message = pickle.loads(client_socket.recv(1024))
  deliver_request(replica, message.data, message) #actually do request
  if message.rq_type == 'CLIENT': #if from the client, not broadcasted from another replica
    #broadcast to the other replicas
    for replica_port in REPLICA_PORTS: #forward message to every other replica
      if replica_port != replica.port:
        send_message(replica_port, Message('REPLICA', message.data, PORT_IDX))

def deliver_request(replica, request, message): #process request in Redis server
  r = REDIS
  if request[0] == 'get':
    result = r.get(request[1])
    if result: result = result.decode()
    print(f'{PORT_IDX}:  ' + ('                                        ' * PORT_IDX) + f'RQ FROM {message.rq_type}{message.src}: GET {request[1]} = {result}') #STATE: {log}')
  else:
    r.set(request[1], request[2])
    print(f'{PORT_IDX}:  ' + ('                                        ' * PORT_IDX) + f'RQ FROM {message.rq_type}{message.src}: SET {request[1]} = {request[2]}') #STATE: {log}')
  log = []
  for key in sorted(r.keys()):
    log.append((key.decode(), int(r.get(key).decode())))

def main():
  global NUM_CLIENTS
  global REPLICA_PORTS
  global REDIS_PORT
  global REDIS
  global PORT_IDX
  global SOCK_TIMEOUT

  PORT_IDX = int(sys.argv[1])
  filesystem = None
  try:
    with open(CONFIG_PATH, 'r') as file:
      filesystem = json.load(file)
  except FileNotFoundError:
    print('Error: Config file config.json is required in main path.')
    sys.exit(0)

  SOCK_TIMEOUT = filesystem['timeout']
  print(f'SOCK_TIMEOUT IS {SOCK_TIMEOUT}')
  REPLICA_PORTS = list(range(filesystem['replica_start_port'], filesystem['replica_start_port'] + filesystem['num_replicas']))
  client_ports = list(range(filesystem['client_start_port'], filesystem['client_start_port'] + filesystem['num_clients']))

  NUM_CLIENTS = filesystem['num_clients']
  PORT = REPLICA_PORTS[PORT_IDX]
  REDIS_PORT = filesystem['redis_ports'][PORT_IDX]
  PROC = subprocess.Popen(f"redis-server --daemonize yes --port {REDIS_PORT} --appendonly yes --appendfilename {PORT_IDX}.aof", shell=True)
  time.sleep(1)
  REDIS = redis.Redis(host='localhost', port=REDIS_PORT, db=0)
  start_replica(PORT) #now that configurations are set, start the replica process
  
if __name__ == '__main__': main()
