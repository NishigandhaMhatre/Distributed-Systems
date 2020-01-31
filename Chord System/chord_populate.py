# -*- coding: utf-8 -*-
"""
Created on Sun Nov 17 21:52:29 2019
@author: Nishigandha Mhatre
Seattle University
Assignment: Distrubuted hash table (chord_populate.py)
"""

import csv
import sys
import hashlib
import pickle
import socket

M = 3 # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
NODES = 2**M
TEST_BASE = 43544
BUF_SZ = 4096 * 4096
       
def read_csv(filename, port):
    """Function to read csv file and pass hash keys to respective nodes"""
    with open(filename) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            key = str(row[0])+str(row[3])
            val = str(row)
            index = getHash(key) % NODES
            call_rpc(port,'find_successor',index)
            successor = call_rpc(port,'find_successor',index)
            call_rpc(successor, 'update_keys_values',key,val)


def look_up(node):
    """Method to look-up port number"""
    return 'localhost', int(node) + TEST_BASE


def getHash(key):
    """Method to generate hash value"""
    hashval = hashlib.sha1(pickle.dumps(key)).hexdigest()
    number = int(hashval, 16)
    return number


def call_rpc(np, reason,key = None, value = None):
    """Method to call rpc"""
    if(reason == 'find_successor'):
        query = ('find_successor',reason, key, value)
    if(reason == 'update_keys_values'):
        query = ('update_keys_values', reason, key, value)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sendr:
        addr = look_up(np)
        sendr.connect(addr)
        sendr.sendall(pickle.dumps(query))
        result = pickle.loads(sendr.recv(BUF_SZ))
    return result
    sendr.close()
        

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: Enter port number of the node to connect and file name (eg.python chord_populate.py 43547 Career_Stats_Passing.csv)")
    else:
        port = int(sys.argv[1])
        filename = sys.argv[2]
        port = port - TEST_BASE
    read_csv(filename, port)