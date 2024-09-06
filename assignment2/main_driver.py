#Spencer Chambers
#Total Order Broadcast Driver Program
#Run this with necessary command line syntax to create Node processes and inject random broadcasts

from lib import Message, Node, send_message
import os, subprocess, sys
import socket
import threading
import time
import pickle
import random
import string

if __name__ == "__main__":
  if len(sys.argv) != 4:
    print('Usage: python3 structure.py <num of Nodes> <num of request> <starting port num>')
    sys.exit(1)
  
  try:
    num_nodes = int(sys.argv[1])
    num_rqs = int(sys.argv[2])
    start_port = int(sys.argv[3])
  except ValueError:
    print("Error: arguments must be integer values.")
    sys.exit(1)

  ports = range(start_port, start_port + num_nodes)
  processes = []
  for port in ports:
    p1 = subprocess.Popen(f"python3 node.py {num_nodes} {num_rqs} {start_port} {port}", shell=True)
    processes.append(p1)
  time.sleep(1)    

  cmds = list(string.ascii_letters)
  for i in range(num_rqs):
    sq = random.randint(1,50)
    msg = Message(sq, 0, 'tom', cmds[i])
    for port in ports: send_message(port, msg)
  
  while len(processes) > 0:
    for p in processes:
      if not (p.poll() is None): processes.remove(p)
