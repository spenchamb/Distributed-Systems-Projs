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
PORT_IDX = None

#OUR PRIMARY REPLICA WILL BE THE PROCESS WITH PORT OF PORT_IDX = 0... JUST A CONVENTION FOR EASE

def exit_handler(): #if exit prompted, close everything out
  global REDIS
  if REDIS: REDIS.shutdown()
  if sock: sock.close()
atexit.register(exit_handler)

def start_replica(port):
  print(f'Starting replica on port {port}...')
  replica = Replica(port)
  replica.sock.settimeout(SOCK_TIMEOUT)
  accept_connections(replica)

def accept_connections(replica): #accept connections and handle timeout behavior (if no requests received for x seconds)
  while True:
    try:
      client_socket, client_address = replica.sock.accept()
      handle_connection(client_socket, replica)
    except socket.timeout:
      break
  print(f'{replica.port}: exiting...')
  replica.sock.close()
  sys.exit(0)

def handle_connection(client_socket, replica): #handle various packet behaviors depending on where at in process
  time.sleep(random.uniform(.02, .1)) #artificial delay for testing purposes
  message = pickle.loads(client_socket.recv(1024)) #receive request from client

  #if the request is a read, no need for anything else.. just locally read
  if message.data[0] == 'get': 
    deliver_request(replica, message.data, message)
    return
 
  #If message is a SET from a client 
  if message.rq_type == 'CLIENT': #if this message comes straight from client, not from redirection of replicas
    #rebroadcast to PRIMARY REPLICA as a FORWARD message
    msg = Message('FORWARD', message.data, message.src_cli)
    msg.src_rep = PORT  
    send_message(REPLICA_PORTS[0], msg)
    return

  if message.rq_type == 'FORWARD': #this will only occur on the primary replica
    #rebroadcast to ALL PORTS as an ACTION message
    #this indicates that when received, the payload should be delivered (redis K/V updated)
    #this is how the PRIMARY orders and allows sequential consistency
    for port in REPLICA_PORTS:
      msg = Message('PRIMARY', message.data, message.src_cli)
      msg.src_rep = message.src_rep
      send_message(port, msg)
    return

  if message.rq_type == 'PRIMARY': #received back from the primary, time to execute
    #deliver this request
    deliver_request(replica, message.data, message)
    #TODO send OK to client if original replica client contacted
    if message.src_rep == PORT: #if this process is the one that initially forwarded it to primary replica
      send_message(message.src_cli, 'OK') #send an OK to the client 

def deliver_request(replica, request, message): #get/set behavior actually carried out
  r = REDIS
  if request[0] == 'get':
    result = r.get(request[1])
    if result: result = result.decode()
    print(f'{PORT_IDX}:  ' + ('                                        ' * PORT_IDX) + f'RQ FROM {message.rq_type}{message.src_cli}: GET {request[1]} = {result}') #STATE: {log}')
  else:
    r.set(request[1], request[2])
    print(f'{PORT_IDX}:  ' + ('                                        ' * PORT_IDX) + f'RQ FROM CLIENT{message.src_cli}: SET {request[1]} = {request[2]}') #STATE: {log}')

def main():
  global NUM_CLIENTS
  global REPLICA_PORTS
  global REDIS_PORT
  global REDIS
  global PORT_IDX
  global PORT
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
  REDIS = redis.Redis(host='localhost', port=REDIS_PORT, db=0)
  start_replica(PORT) #begin replica behavior
  
if __name__ == '__main__': main()
