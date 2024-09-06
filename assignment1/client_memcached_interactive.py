#Spencer Chambers
#B534 Assignment 1
#Client using PYMEMCACHE

from pymemcache.client.base import Client #using pymemcache as our example client
from pymemcache.exceptions import MemcacheUnexpectedCloseError
import socket
import sys
import time
import re


def send_memcached(host, port, cmd):
  client = Client(f"{host}:{port}")
  if cmd[0] == 'get':
    try:
      a = client.get(cmd[1][0]).decode() #PYMEMCACHE command!
    except MemcacheUnexpectedCloseError: #This occurs when server sends None (GET on nonexistent value)
      a = None
    print(a)
  else:
    a = client.set(cmd[1][0], cmd[1][1]) #PYMEMCACHE command!
    if a: print('STORED')

if __name__ == "__main__":

  if len(sys.argv) != 3: #error handling
    print('Error: Must input args in following fashion: python3 client_memcached.py <host> <port>')
    sys.exit(1)
  
  host = sys.argv[1]

  try:
    port = int(sys.argv[2])
  except ValueError: #error handling if portnum arg isn't a number
    sys.exit(1)

  getset, key, value, cmd = None, None, None, None
  print("Syntax:\nget <key>\nset <key> <'value'> (NEED SINGLE QUOTES!)")
  while cmd != True:
    cmd = input("Enter command, or enter 'end' to end: ")
    if cmd == 'end': sys.exit(0)
    getset = cmd.split(' ')[0]
    key = cmd.split(' ')[1]
    value = cmd[cmd.find("'") + 1: cmd.rfind("'")]
    send_memcached(host, port, [getset, [key, value]])
