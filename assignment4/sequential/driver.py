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
  
  procs = [] #holds active processes
  for i in range(filesystem['num_replicas']):
    p1 = subprocess.Popen(f'python3 replica.py {i}', shell=True)
    procs.append(p1)

  time.sleep(2) #like other schemes, necessary for allowing replica to boot up. necessary because working on single system.

  for i in range(filesystem['num_clients']):
    procs.append(subprocess.Popen(f'python3 client.py {i}', shell=True))
  
  while len(procs) > 0:
    for p in procs:
      if not (p.poll() is None): procs.remove(p)
  
  timeout = filesystem['timeout']
  end_time = time.time()
  print(f'TOTAL TIME ELAPSED: {end_time - start_time - timeout} seconds')
  sys.exit(0)

if __name__ == "__main__": main()
