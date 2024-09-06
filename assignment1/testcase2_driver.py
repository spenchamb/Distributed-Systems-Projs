import socket
import sys
import os
import subprocess


#######TEST CASE 2#########
#Creates multiple clients with unique commands to test edge case behaviors

if __name__ == "__main__":
    if len(sys.argv) != 3: sys.exit(1)

    host = sys.argv[1]
    
    try:
        port = int(sys.argv[2])
    except ValueError:
        sys.exit(1)
    
    #For best testing, we want to start with a clean slate. You can delete the data.json file
    #and the server will create a new one automatically.
  
    numClients = input("Enter the number of clients you wish to make: ")
    
    for i in range(int(numClients)):
      p1 = subprocess.Popen(f"python3 tc2-files/tc2-client.py {host} {port}", shell = True)
