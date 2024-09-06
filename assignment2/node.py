#Spencer Chambers
#Node object program: holds algorithm specific behavior and represents a single Node in the system
#You can read this program almost perfectly up and down: Start with start_node, then each function calls the next.

from lib import Message, Node, send_message
import socket
import threading
import sys
import time
import pickle
import random

ports = []
num_nodes = None
start_port = None
port = None
write_lock = threading.Lock()
queue = []
num_rqs = None
ct = 0

def start_node(port):
  node = Node(port)
  accept_connections(node)
  global queue  

def accept_connections(node):
  while ct < num_rqs:
    client_socket, client_address = node.sock.accept()
    handle_connection(client_socket, node)
  node.sock.close()
  sys.exit(0)

def handle_connection(client_socket, node):
  #with write_lock:
    message = pickle.loads(client_socket.recv(1024))

    #ARTIFICIAL DELAY INSERTED HERE! DELETE FOR TESTING PURPOSES, IF DESIRED
    #Include the snippet at end of next line to differing delay amounts between nodes
    time.sleep(random.random() * .05 )#* ports.index(node.port))
    
    node.lc_time = max(node.lc_time, message.sq_num) + 1
    queue.append(message)
    if message.rq_type == 'tom': broadcast_ack(message, node)
    check_queue(node)

def check_queue(node):
  global queue
  global num_nodes
  global ct
  while len(queue) > 0 and queue[0].rq_type == 'tom':
    queue, temp_ports, payload, temp = sorted(queue), list(ports), None, []  
    payload = queue[0]
    for msg in queue:
      if msg.rq_type == 'ack' and msg.data == queue[0].data:
        temp_ports.remove(msg.src_node)
    if len(temp_ports) == 0:
      for msg in queue:
        if msg.data != payload.data: temp.append(msg) 
      queue = temp
      out = list(f"Node{node.port}-> #" + ('         #' * num_nodes))
      out[16 + (10 * ports.index(node.port))] = payload.data #THIS IS ALL JUST FORMATTING FOR OUTPUT. DOESN'T MATTER
      print(''.join(out))
      ct = ct + 1
      #print(("     " * ports.index(node.port))+ f"Node {node.port} DELIVERS: {payload.data}")
    else: break
  
def broadcast_ack(original_message, node):
  node.lc_time = node.lc_time + 1
  ack = Message(node.lc_time, node.port, 'ack', original_message.data)
  broadcast(ack)

def broadcast(message):
  for port in ports: send_message(port, message)

##################################################################################
if __name__ == "__main__":
  
  try:
    num_nodes = int(sys.argv[1])
    num_rqs = int(sys.argv[2])
    start_port = int(sys.argv[3])
    port = int(sys.argv[4])
  except ValueError:
    print("Error: arguments must be integer values.")
    sys.exit(1)
  
  ports = range(start_port, start_port + num_nodes)
  start_node(port)
  
