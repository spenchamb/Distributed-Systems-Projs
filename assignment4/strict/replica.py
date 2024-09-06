import sys
import pickle
import json
import os, subprocess
import time
import redis
import random
import atexit
from lib import *

CONFIG_PATH = 'config.json'
PORT = None
REDIS_PORT = None
PROC = None
r = None
queue = []
sock = None
NUM_CLIENTS = 0
REPLICA_PORTS = []
SOCK_TIMEOUT = None
PORT_IDX = 0

def exit_handler(): #if set to exit, close everything out
  if r: r.shutdown()
  if sock: sock.close()
atexit.register(exit_handler)

def start_replica(r, port):
  print(f'Starting replica on port {port}...')
  replica = Replica(port)
  replica.sock.settimeout(SOCK_TIMEOUT) #configurable timeout value when no requests have been received on a replica to close
  global sock
  sock = replica.sock
  accept_connections(replica)
  global queue

def accept_connections(replica): #accepts connections and passes them on to next function. handles timeout
  while True:
    try:
      client_socket, client_address = replica.sock.accept()
      handle_connection(client_socket, replica)
    except socket.timeout: #if SOCK_TIMEOUT seconds go by without a request, close out.
      #this was the best way I could figure to close uniformly
      break
  print(f'{replica.port}: exiting...')
  replica.sock.close()
  sys.exit(0)

def handle_connection(client_socket, replica): #unpacks message and handles
  time.sleep(random.uniform(.02, .1)) #ARTIFICIAL DELAY PER ASSIGNMENT SUGGESTIONS
  message = pickle.loads(client_socket.recv(1024))
  if message.rq_type == 'cli': #received message from client.. NOT to be entered into queue
    #broadcast this message to all replicas, included this one
    broadcast_request(message, replica)
    return

  #below is TOM behavior
  replica.lc_time = max(replica.lc_time, message.sq_num) + 1
  queue.append(message)
  if message.rq_type == 'tom': broadcast_ack(message, replica)
  check_queue(replica) 

def check_queue(replica): #sorts queue and removes/delivers any requests that are due (head of queue and all acks in queue)
  global queue
  num_replicas = len(REPLICA_PORTS)
  while len(queue) > 0 and queue[0].rq_type == 'tom':
    queue, temp_ports, payload, temp = sorted(queue), list(REPLICA_PORTS), None, []
    payload = queue[0]
    for msg in queue:
      if msg.rq_type == 'ack' and msg.data == queue[0].data:
        temp_ports.remove(msg.src_node)
    if len(temp_ports) == 0:
      for msg in queue:
        if msg.data != payload.data: temp.append(msg)
      queue = temp
      #DELIVERY IS IN PAYLOAD
      deliver_request(replica, payload.data)
    else: break
   
def deliver_request(replica, request): #actually does the setting/getting in REDIS port
  r = redis.Redis(host='localhost', port=REDIS_PORT, db=0)
  if request[0] == 'get':
    result = r.get(request[1])
    if result: result = result.decode()
  else:
    r.set(request[1], request[2])
  log = []
  for key in sorted(r.keys()):
    log.append((key.decode(), r.get(key).decode()))
  if request[0] == 'get':
    print(f'{PORT_IDX}:  ' + ('            ' * PORT_IDX) + f'GET {request[1]} = {result}') #STATE: {log}')
  else:
    print(f'{PORT_IDX}:  ' + ('            ' * PORT_IDX) + f'SET {request[1]} = {request[2]}') #STATE: {log}')
   

def broadcast_request(message, replica): #take message from client and put it into TOM
  replica.lc_time = replica.lc_time + 1
  msg = Message(replica.lc_time, replica.port, 'tom', message.data)
  broadcast(msg)

def broadcast_ack(original_message, replica): #self explanatory, send ack to all ports of a given message
  replica.lc_time = replica.lc_time + 1
  ack = Message(replica.lc_time, replica.port, 'ack', original_message.data)
  broadcast(ack)

def broadcast(message):
  for port in REPLICA_PORTS: send_message(port, message)

def main():
  global r
  global NUM_CLIENTS
  global REPLICA_PORTS
  global REDIS_PORT
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
  REPLICA_PORTS = list(range(filesystem['replica_start_port'], filesystem['replica_start_port'] + filesystem['num_replicas']))
  client_ports = list(range(filesystem['client_start_port'], filesystem['client_start_port'] + filesystem['num_clients']))
  NUM_CLIENTS = filesystem['num_clients']
  PORT = REPLICA_PORTS[PORT_IDX]
  REDIS_PORT = filesystem['redis_ports'][PORT_IDX]
  PROC = subprocess.Popen(f"redis-server --daemonize yes --port {REDIS_PORT} --appendonly yes --appendfilename {PORT_IDX}.aof", shell=True)
  time.sleep(1)
  r = redis.Redis(host='localhost', port=REDIS_PORT, db=0)

  start_replica(r, PORT)
  
if __name__ == '__main__': main()
