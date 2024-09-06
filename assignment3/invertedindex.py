#Spencer Chambers
#MapReduce: Inverted Index

from lib import *
import socket
import os, subprocess, sys
import json
import pickle
import string
import re
import time

CONFIG_PATH = 'config.json'

#takes initial cache of maps and organizes it into num_reducers number of subarrays, sorted alphabetically.
#this ensures all traffic to some reducer is within the same alphabetical range in all mappers. consistency!
def __wcgroupmaps(maps, num_reducers):
  start = 97
  abc_ranges = []
  rangesize = 26 // num_reducers
  leftover = 26 % num_reducers
  for i in range(num_reducers):
    abc_ranges.append(list(range(start, start + rangesize)))
    start += rangesize
  abc_ranges[-1] += list(range(start, start + leftover))
  for i in range(len(abc_ranges)):
    for j in range(len(abc_ranges[i])):
      abc_ranges[i][j] = chr(abc_ranges[i][j])
  res = []
  for i in range(num_reducers): res.append([])
  for mp in maps:
    for i in range(num_reducers):
      if mp[0][0] in abc_ranges[i]: res[i].append(mp)
  return res  

#sends maps found to corresponding reducers
def send_maps(grouped_maps, reducer_ports, port):
  print('send_maps accessed')
  for i in range(len(reducer_ports)):
    for j in range(len(grouped_maps[i])):
      send_message(reducer_ports[i], grouped_maps[i][j])
  print('Node done sending to reducer.')

#parse the given partition of files, find words, add to cache, return cache
def __generate_maps(partition, readable_files):
  maps = []
  all_lines = []
  my_lines = []
  for i in range(len(readable_files)):
    with open(readable_files[i], 'r') as file:
      this_files_lines = file.readlines()
      for line in this_files_lines:
        line = [line, i]
        all_lines.append(line)
  start = 0
  for i in range(partition[1]):
    start += partition[0][i]
  for i in range(partition[0][partition[1]]):
    my_lines.append(all_lines[i + start])
  for i in range(len(my_lines)):
    for word in my_lines[i][0].split():
      if (not any(char.isalpha() for char in word)): continue
      if (not word[-1].isalpha()): word = word[:-1]
      if (not word[0].isalpha()): word = word[1:]
      word = re.sub(r'[^a-zA-Z\']+', '', word)
      maps.append([word.lower(), 1, my_lines[i][1]])
  return maps

#write to the results folder with given filename syntax
def _write_to_file(result_dict, port):
  filepath = f"results/invertedindex{port}.txt"
  with open(filepath, 'w') as file:
    for key, value in result_dict.items():
      file.write(f'{key}: {value}\n')
    print(f'Reducer{port}: See results in results/invertedindex{port}.txt')

#ACTUAL REDUCE BEHAVIOR
def _do_reduce(maps):
  result_dict = {}
  for mapp in maps:
    if not (mapp[0] in result_dict):
      result_dict[mapp[0]] = {}
      result_dict[mapp[0]][mapp[2]] = 1
    else: #if word has been found before
      try:
        result_dict[mapp[0]][mapp[2]] += 1
      except KeyError: #word hasn't been found in a given doc yet
        result_dict[mapp[0]][mapp[2]] = 1
  return result_dict

#######################################################################3
#MAP AND REDUCE FUNCTIONS
def iimap(port, cfg):
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.bind(('localhost', port))
  sock.listen(5)
  print(f"Mapper listening on port {port}...")

  client_socket, addr = sock.accept()
  reducer_ports = pickle.loads(client_socket.recv(1024)) #array of mapper ports received from master

  client_socket, addr = sock.accept()
  partition = pickle.loads(client_socket.recv(1024)) #partition received from master
  print(f"partition received by master for mapper{port}:{partition}")

  #fill array of words from selected lines
  maps = __generate_maps(partition, cfg["files"])

  #Group to correct mappers. We do this alphabetically!  
  grouped_maps = __wcgroupmaps(maps, len(reducer_ports))
  send_message(MASTER_PORT, f'MAPPER{port} DONE MAPPING')

  #BARRIER: Here we wait for confirmation from Master Node that reducers can be reached
  client_socket, addr = sock.accept()
  go_msg = None
  while go_msg != 'GO':
    go_msg = pickle.loads(client_socket.recv(1024))
  print('MAPPER READY TO SEND TRAFFIC TO REDUCER')
  time.sleep(.2) #constraint due to local system. if this were truly distributed, no need for this

  #Send num of entries to reducers
  for i in range(len(reducer_ports)):
    send_message(reducer_ports[i], len(grouped_maps[i]))

  time.sleep(.6) #DUE TO CONTSTRAINTS OF SINGLE SYSTEM. WAITING FOR REDUCER PROCESSES TO BE READY

  #Send all traffic to maps
  send_maps(grouped_maps, reducer_ports, port)
  print(f"MAPPER{port} EXITS.")
  sock.close()
  sys.exit(0)

def iireduce(port, cfg, num_mappers):
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.bind(('localhost', port))
  sock.listen(5)
  print(f"Reducer listening on port {port}...")

  num_entries_arr = []
  maps = []
  nes = 0

  #Receive number of entries to expect from each mapper (necessary for termination of listening)
  while(len(num_entries_arr) < num_mappers):
    client_socket, addr = sock.accept()
    mapnum = pickle.loads(client_socket.recv(1024))
    if isinstance(mapnum, int): num_entries_arr.append(mapnum)
    else:
      print('BAD') 
      maps.append(mapnum)
      nes -= 1

  print(f"Number of non-unique entries expected by reducer{port}: {num_entries_arr}")
  data = b""
  done_ct = 0
  nes += sum(num_entries_arr)

  #Receive packets from Mapper and add to maps cache array
  while done_ct < nes:
    client_socket,addr = sock.accept() 
    pack = client_socket.recv(4096)
    done_ct += 1
    entry = pickle.loads(pack)
    maps.append(entry)

  print(f"Reducer{port} done receiving traffic.")  

  #Use our filled Maps data to do REDUCE behavior
  result_dict = _do_reduce(maps)  

  #sort based on number of instances in all files
  result_dict = dict(sorted(result_dict.items(), key=lambda d: sum(d[1].values()), reverse=True))

  #Write Reducer output to file named after Reducer's port number!
  _write_to_file(result_dict, port)
  sock.close()
  sys.exit(0)

if __name__ == "__main__":
  with open(CONFIG_PATH, 'r') as file:
      cfg = json.load(file)

  port = int(sys.argv[2])
  MASTER_PORT = int(sys.argv[3])

  if int(sys.argv[1]) == 0:
    iimap(port, cfg)
  else:
    num_mappers = int(sys.argv[4])
    iireduce(port, cfg, num_mappers)  
