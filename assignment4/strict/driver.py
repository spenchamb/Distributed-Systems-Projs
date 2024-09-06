import sys
import pickle
import os, subprocess
import json
import time
import atexit

CONFIG_PATH = 'config.json'
procs = None

def exit_handler():
  if procs:
    for proc in procs: proc.kill()
atexit.register(exit_handler)

def main():
  start_time = time.time()
  global procs
  filesystem = None
  try:
    with open(CONFIG_PATH, 'r') as file:
      filesystem = json.load(file)
  except FileNotFoundError:
    print('Error: Config file config.json is required in main path.')
    sys.exit(0)

  replica_ports = list(range(filesystem['replica_start_port'], filesystem['replica_start_port'] + filesystem['num_replicas']))
  client_ports = list(range(filesystem['client_start_port'], filesystem['client_start_port'] + filesystem['num_clients']))
  
  procs = [] #list of open processes
  for i in range(filesystem['num_replicas']):
    p1 = subprocess.Popen(f'python3 replica.py {i}', shell=True)
    procs.append(p1)

  time.sleep(2) #consistent delay on all consistency schemes to let replicas get ready to receive before clients begin.
  #this is due to a constraint of making this work on a single system

  for i in range(filesystem['num_clients']):
    procs.append(subprocess.Popen(f'python3 client.py {i}', shell=True))
  
  while len(procs) > 0: #while there's an open process
    for p in procs:
      if not (p.poll() is None): procs.remove(p)

  end_time = time.time()
  timeout = filesystem['timeout']
  print(f'TOTAL TIME ELAPSED: {end_time - start_time - timeout} seconds')
  sys.exit(0)

if __name__ == "__main__": main()
