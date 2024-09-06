#Spencer Chambers
#MapReduce: MapReduce 'api' or library of functions

import socket
import os, subprocess, sys
import json
from multiprocessing import Process
import pickle
import random

#retrieve config file settings
def fill_filesystem(cfg_path):
  #Open config file
  filesystem = None
  try:
    with open(cfg_path, 'r') as file:
      filesystem = json.load(file)
  except FileNotFoundError:
    print('Error: Config file config.json is required in main path.')
    sys.exit(0)
  return filesystem

def find_line_counts(files):
  line_counts = []
  for filename in files:
    with open(filename, 'r', encoding='utf-8') as file:
      line_count = sum(1 for line in file)
    line_counts.append(line_count)
  return line_counts

#given line counts of some files, find a mostly even partition of line numbers
def partition(line_counts, num_mappers):
  total_line_count = sum(line_counts)
  parts = []
  for i in range(num_mappers):
    parts.append(total_line_count // num_mappers)
  parts[-1] += total_line_count % num_mappers
  return parts

def create_socket(port):
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.bind(('localhost', port))
  sock.listen(5)
  return sock

def generate_ports(num_mappers, num_reducers):
  #Randomly select ports for entire operation
  all_ports = set()
  while len(all_ports) < (1 + num_mappers + num_reducers):
    all_ports.add(random.randint(4000, 8000))
  all_ports = list(all_ports)
  master_port = all_ports[0]
  map_ports = all_ports[1:1+num_mappers]
  reduce_ports = all_ports[1+num_mappers:]
  return master_port, map_ports, reduce_ports

#actually start process to do mapper task
def start_mappers(master_port, map_ports, num_mappers, fn):
  procs = []
  for i in range(num_mappers):
    proc = subprocess.Popen(f"python3 {fn} 0 {map_ports[i]} {master_port}", shell=True)
    procs.append(proc)
  return procs

#actually start process to do reducer task
def start_reducers(num_mappers, master_port, reducer_ports, num_reducers, fn):
  procs = []
  for i in range(num_reducers):
    proc = subprocess.Popen(f"python3 {fn} 1 {reducer_ports[i]} {master_port} {num_mappers}", shell=True)
    procs.append(proc)
  return procs

#simple create socket, send message to given port, delete
def send_message(port, message):
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
    client_socket.connect(('localhost', port))
    client_socket.sendall(pickle.dumps(message))

