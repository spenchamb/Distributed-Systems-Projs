#Spencer Chambers
#MapReduce Master Node Program

from lib import *
import os, subprocess, sys
import socket
import threading
import string
import json
import random
import time
import pickle

CONFIG_PATH = 'config.json'

def main():
  start_time = time.time()
  
  #Command line argument catching
  if len(sys.argv) != 1:
    print('Usage: python3 master.py\nEdit config file for testing different conditions.')
    sys.exit(0)
  
  #Using JSON files, fill up a dictionary with the info from shared config file
  filesystem = fill_filesystem(CONFIG_PATH)
  num_mappers, num_reducers, fn = filesystem["num_mappers"], filesystem["num_reducers"], filesystem["fn"]

  #Randomly select ports for entire operation
  master_port, map_ports, reduce_ports = generate_ports(num_mappers, num_reducers)

  #Create Master Node socket at designated port (used for receiving messages from mappers)
  sock = create_socket(master_port)
  print(f"Master Node listening on port {master_port}...") 
 
  #Feed api list of files and how many mappers to divide amongst, and return line ranges that each mapper should read.
  #Supports multiple document partitioning (if partition range goes into another round, it will)
  partitions = partition(find_line_counts(filesystem["files"]), num_mappers)
  print(f"master partitions: {partitions}")

  #Start mapper processes with minimal shared state in args, and sleep for short time before sending the partitions we found
  procs = start_mappers(master_port, map_ports, num_mappers, fn)
  time.sleep(1)

  #Send the array of reducer ports to each mapper (this is to reduce shared state. still very low traffic)
  for i in range(num_mappers):
    send_message(map_ports[i], reduce_ports)

  #Send the partition ranges of files (assume they can handle this due to lack of distributed file system.)
  for i in range(num_mappers):
    send_message(map_ports[i], [partitions, i]) #send partition ranges to mappers to begin

  #Start reducer processes, that will wait for traffic from the mappers.
  procs = procs + start_reducers(len(map_ports), master_port, reduce_ports, num_reducers, fn) #start reducers that will wait until mappers are done

  #BARRIER! ACCEPT NUM_MAPPER # OF 'DONE' MESSAGES. THIS WILL CONFIRM ALL MAPPERS ARE FINISHED BEFORE WE ALLOW THEM TO SEND TRAFFIC TO REDUCERS
  done_ct = 0
  while done_ct < num_mappers:
    client_socket, addr = sock.accept()
    data = pickle.loads(client_socket.recv(1024))
    print(data)
    done_ct += 1
  
  #Passed the barrier, so send the 'GO' message to Mappers to start sending to reducers
  for i in range(num_mappers):
    send_message(map_ports[i], 'GO')

  #Simply ensures Master Node does not exit when some mapper/reducer is still active
  while len(procs) > 0:
    for p in procs:
      if not (p.poll() is None): procs.remove(p)

  end_time = time.time()
  print(f'TOTAL TIME ELAPSED: {end_time - start_time} seconds')
   
if __name__ == "__main__": main()
