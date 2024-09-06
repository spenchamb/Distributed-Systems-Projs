#Spencer Chambers
#B534 Assignment 1
#Client using PYMEMCACHE

from pymemcache.client.base import Client #using pymemcache as our example client
import socket
import sys
import time


def send_memcached(host, port, cmd):
  client = Client(f"{host}:{port}")
  if cmd[0] == 'get':
    a = client.get(cmd[1][0]).decode() #PYMEMCACHE command!
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

  #list of commands that will be executed by helper function above.
  cmds = [['get', ['key1']],
          ['set', ['key1', 'val_1']]
          ,['get', ['sunrise']]
          #['set', ['sunrise', 'sunset']],
          ]
  
  for cmd in cmds: send_memcached(host, port, cmd) #call helper function on all commands
