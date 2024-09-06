import socket
import sys
import os
import subprocess


#######TEST CASE 1#########
#Creates multiple clients with unique commands to test edge case behaviors

if __name__ == "__main__":
    if len(sys.argv) != 3: sys.exit(1)

    host = sys.argv[1]
    
    try:
        port = int(sys.argv[2])
    except ValueError:
        sys.exit(1)

    delay = input("Input 0 for no delay, 1 for delay: ")
    
    #For best testing, we want to start with a clean slate. You can delete the data.json file
    #and the server will create a new one automatically.

    p1 = subprocess.Popen(f"python3 tc1-files/tc1-client1.py {host} {port} {delay}", shell = True)
    p2 = subprocess.Popen(f"python3 tc1-files/tc1-client2.py {host} {port} {delay}", shell = True)
