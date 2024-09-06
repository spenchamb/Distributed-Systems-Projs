#Spencer Chambers
#MapReduce: Word Count

from lib import *
import socket
import os, subprocess, sys
import json
import pickle
import string
import re
import time

CONFIG_PATH = 'config.json'

#group maps into num_reducers number of subarrays, sorted alphabetically.
#this ensures each reducer will receive the same range of words!
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

#send data we've mapped to reducers
def send_maps(grouped_maps, reducer_ports, port):
  print(f'Mapper{port} sending maps to reducers.')
  for i in range(len(reducer_ports)):
    for j in range(len(grouped_maps[i])):
      send_message(reducer_ports[i], grouped_maps[i][j])
  print(f'Mapper{port} done sending to reducers.')

#given partition, parse file to retrieve words
def __generate_maps(partition, readable_files):
  maps = []
  all_lines = []
  my_lines = []
  for i in range(len(readable_files)):
    with open(readable_files[i], 'r') as file:
      all_lines.extend(file.readlines())
  start = 0
  for i in range(partition[1]):
    start += partition[0][i]
  for i in range(partition[0][partition[1]]):
    my_lines.append(all_lines[i + start])
  for i in range(len(my_lines)):
    for word in my_lines[i].split():
      if (not any(char.isalpha() for char in word)): continue
      if (not word[-1].isalpha()): word = word[:-1]
      if (not word[0].isalpha()): word = word[1:]
      word = re.sub(r'[^a-zA-Z\']+', '', word)
      maps.append([word.lower(), 1])
  return maps

#Write result to file of correct filename syntax
def _write_to_file(result_dict, port):
  filepath = f"results/wordcount{port}.txt"
  with open(filepath, 'w') as file:
    for key, value in result_dict.items():
      file.write(f'{key}: {value}\n')
    print(f'Reducer{port}: find results at results/wordcount{port}.txt')


#################################################################################
#MAP AND REDUCE FUNCTIONS
def wcmap(port, cfg):
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.bind(('localhost', port))
  sock.listen(5)
  print(f"Mapper listening on port {port}...")

  client_socket, addr = sock.accept()
  reducer_ports = pickle.loads(client_socket.recv(1024)) #array of mapper ports received from master

  client_socket, addr = sock.accept()
  partition = pickle.loads(client_socket.recv(1024)) #partition received from master
  print(f"partition received my mapper:{partition}")

  #fill array of words from selected lines
  maps = __generate_maps(partition, cfg["files"])

  #Group to correct mappers. We do this alphabetically!  
  grouped_maps = __wcgroupmaps(maps, len(reducer_ports))
  send_message(MASTER_PORT, 'MAPPER DONE MAPPING')

  #BARRIER: Here we wait for confirmation from Master Node that reducers can be reached
  client_socket, addr = sock.accept()
  go_msg = None
  while go_msg != 'GO':
    go_msg = pickle.loads(client_socket.recv(1024))
  print('MAPPER READY TO SEND TRAFFIC TO REDUCER')
  time.sleep(.8) #constraint of same-system simulation of distributed environment.

  #Send num of entries to reducers
  for i in range(len(reducer_ports)):
    send_message(reducer_ports[i], len(grouped_maps[i]))

  time.sleep(.8) #DUE TO CONTSTRAINTS OF SINGLE SYSTEM. WAITING FOR REDUCER PROCESSES TO BE READY
  #Send all traffic to maps
  send_maps(grouped_maps, reducer_ports, port)
  print(f"MAPPER{port} EXITS.")
  sock.close()
  sys.exit(0)

def wcreduce(port, cfg, num_mappers):
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.bind(('localhost', port))
  sock.listen(5)
  print(f"Reducer listening on port {port}...")

  num_entries_arr = []
  maps = []
  nes = 0

  #Receive number of entries expected from each mapper
  while(len(num_entries_arr) < num_mappers):
    client_socket, addr = sock.accept()
    mapnum = pickle.loads(client_socket.recv(1024))
    if isinstance(mapnum, int): num_entries_arr.append(mapnum)
    else:
      print('this should not happen')
      maps.append(mapnum)
      nes -= 1

  data = b""
  done_ct = 0
  nes += sum(num_entries_arr)
  #print(f'{port}nes is {nes}')

  #Receive packets from mapper node!
  while done_ct < nes:
    client_socket,addr = sock.accept() 
    pack = client_socket.recv(4096)
    done_ct += 1
    entry = pickle.loads(pack)
    maps.append(entry)

  print(f"Reducer{port} done receiving traffic.")
  
  #Use our filled Maps data to do REDUCE behavior
  result_dict = {}
  for mapp in maps:
    if not (mapp[0] in result_dict): result_dict[mapp[0]] = 1
    else: result_dict[mapp[0]] += 1 

  #Sort descending by word count 
  result_dict = dict(sorted(result_dict.items(), key=lambda item: item[1], reverse=True))

  #Write Reducer output to file named after Reducer's port number!
  _write_to_file(result_dict, port)

if __name__ == "__main__":
  with open(CONFIG_PATH, 'r') as file:
      cfg = json.load(file)

  port = int(sys.argv[2])
  MASTER_PORT = int(sys.argv[3])

  if int(sys.argv[1]) == 0:
    wcmap(port, cfg)
  else:
    num_mappers = int(sys.argv[4])
    wcreduce(port, cfg, num_mappers)  
