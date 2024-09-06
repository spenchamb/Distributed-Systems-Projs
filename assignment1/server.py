#Spencer Chambers
#CSCIB534 Assignment 1
#Server

import socket
import json
import sys
import time
import random
import threading

FILE = 'data.json' #the local file we will be modifying
write_lock = threading.Lock() #global lock for synchronous gets/sets

#########################################################################################
def do_request(client_socket, data):
    time.sleep(random.uniform(0, 1)) #random time of sleep for interesting test cases

    if data.startswith("GET") or data.startswith("get"):
        command, key = data.split()
        value = filesystem.get(key, "Not found")
        if value == "Not found":
          response = None #Like regular Memcache, send None if attempting a GET on a nonexistant value
        else: 
          response = f"VALUE {key} 0 {len(value)}\r\n{value}\r\nEND\r\n"
        response = str(response).encode()

    elif data.startswith("SET") or data.startswith("set"):
        lines = data.split('\r\n')
        line1 = lines[0]
        line1arr = line1.split(' ') #array of each individual element on first line

        value = lines[1] #entire second line is just the value of this pair
        key = line1arr[1]
        valuesize = line1arr[-2] #this allows compatability with flags/NOREPLY part of memcache
        with write_lock: #locks every other thread while this one modifies the file!
            filesystem[key] = value
            # Update data with the new pair
            with open(FILE, 'w') as file:
                json.dump(filesystem, file) #this will either modify an existing data.json, or create a new one
        response = "STORED".encode()
    else:
        response = "Invalid command".encode() #handles incorrect requests from manual client usage

    client_socket.send(response) #send the server's response back to client, and close
    client_socket.close()

##########################################################################################
def server(host, port): #handles new connections and thread creation
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)

    print(f"Server listening on {host}:{port}...")

    while True:
        client_socket, addr = server_socket.accept() #receive incoming traffic from client
        data = client_socket.recv(1024).decode().strip()
        print(data)
        # Create a new thread of the above function for each request, so requests can be done concurrently!
        threading.Thread(target=do_request, args=(client_socket, data)).start()

##########################################################################################
if __name__ == "__main__":

    #Open the source data file
    try:
      with open(FILE, 'r') as file:
        filesystem = json.load(file)
    except FileNotFoundError:
      filesystem = {}

    # Check if a port argument is provided
    if len(sys.argv) != 3:
        print("Usage: python3 server.py <host> <port>")
        sys.exit(1)

    host = sys.argv[1]

    try:
        port = int(sys.argv[2])
    except ValueError: #if port number arg isn't a number
        print("Error: Port must be an integer.")
        sys.exit(1)

    try:
        server(host, port)
    except socket.gaierror: #if host arg isn't valid
        print("Error: Unacceptable host.")
        sys.exit(1)

