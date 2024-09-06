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
    #self.lc_time = 0
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #ADDED LINE BELOW TO GIVE LENIENCY TO PORT USAGE
    self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.sock.bind(('localhost', port))
    self.sock.listen(100)

class Message:
  def __init__(self, rq_type, data, src_cli):
    self.src_cli = src_cli
    self.src_rep = None
    self.rq_type = rq_type
    self.data = data

  def __repr__(self):
    return f'[{self.rq_type} {self.src_cli} {self.src_rep} {self.data}]'
    #return f"From{self.src_node} SQ{self.sq_num} RQ:{self.rq_type} {self.data}"
