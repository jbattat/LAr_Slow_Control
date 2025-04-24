import sys
#import Doberman
import subprocess
#import time
#import os
#import threading
#import json
#import datetime
#import zmq
#from heapq import heappush, heappop

#cmd = 'ssh doberman@revpi-core "cd /global/software/doberman/scripts && ./start_process.sh -d RevPi1"'
cmd = 'ssh doberman@revpi-core "touch do_you_see_me.txt"'
print(cmd)

try:
    cp = subprocess.run(cmd, shell=True, capture_output=True, timeout=30)
except subprocess.TimeoutExpired:
    print(f'Command timed out!')
if cp.stdout:
    print(f'Stdout: {cp.stdout.decode()}')
if cp.stderr:
    print(f'Stderr: {cp.stderr.decode()}')
