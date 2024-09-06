import socket
import pickle
import json
import sys
import time

#simple create socket, send message to given port, delete
def send_message(port, message):
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
    client_socket.connect(('localhost', port))
    client_socket.sendall(pickle.dumps(message))

class Replica:
  def __init__(self, port):
    self.port = port
    self.lc_time = 0
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #ADDED LINE BELOW TO GIVE LENIENCY TO PORT USAGE
    self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.sock.bind(('localhost', port))
    self.sock.listen(100)

class Message:
  def __init__(self, sq_num, src_node, rq_type, data):
    self.src_node = src_node
    self.rq_type = rq_type
    self.data = data
    self.sq_num = sq_num

  def __lt__(self, other):
    if self.sq_num < other.sq_num: return True
    if self.sq_num == other.sq_num:
      if self.src_node < other.src_node: return True
    return False

  def __repr__(self):
    return f"From{self.src_node} SQ{self.sq_num} RQ:{self.rq_type} {self.data}"
