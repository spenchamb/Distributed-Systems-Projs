#Spencer Chambers
#helper classes and functions

import socket
import sys
import pickle

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

class Node:
  def __init__(self, port):
    self.port = port
    self.lc_time = 0

    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.sock.bind(('localhost', port))
    self.sock.listen(100)

def send_message(port, message):
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
    client_socket.connect(('localhost', port))
    client_socket.sendall(pickle.dumps(message))
