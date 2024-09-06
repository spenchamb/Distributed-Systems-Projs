from pymemcache.client.base import Client
import socket
import sys
import time

def send_request(host, port, cmd):
  client = Client(f"{host}:{port}")
  if cmd[0] == 'get':
    a = client.get(cmd[1][0]).decode() #PYMEMCACHE command!
    print(a)
  else:
    a = client.set(cmd[1][0], cmd[1][1]) #PYMEMCACHE command!
    if a: print('STORED')

if __name__ == "__main__":
    host = sys.argv[1]
    port = int(sys.argv[2])
    delay = int(sys.argv[3])
    
    if delay: time.sleep(.5)
    send_request(host, port, ['get', ['key1']])
    if delay: time.sleep(1)
    send_request(host, port, ['get', ['key1']])
    if delay: time.sleep(1)
    send_request(host, port, ['get', ['key1']])
    if delay: time.sleep(1)
    send_request(host, port, ['get', ['key1']])
    if delay: time.sleep(1)
    send_request(host, port, ['get', ['key1']])
    if delay: time.sleep(1)
