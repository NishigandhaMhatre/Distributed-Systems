# -*- coding: utf-8 -*-
"""
Created on Sun Nov 17 21:52:29 2019
@author: Nishigandha Mhatre
Seattle University
Assignment: Distrubuted hash table (chord_query.py)
"""
import sys
import hashlib
import pickle
import socket

M = 3  # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
NODES = 2**M
TEST_BASE = 43544
BUF_SZ = 4096 * 4096

def find_value_for_key(port, key):
    """Function to find value for the key"""
    index = getHash(key) % NODES
    successor = call_rpc(port, 'find_successor', index)
    print("Successor of the key: ", successor)
    if successor:
        value = call_rpc(successor, 'find_value',key)
        if value:
            print("Value of Key {} is {}". format(key,value))
        else:
            print("Key not found")
            
            
def call_rpc(np, reason,key = None, value = None):
    """Method to call rpc"""
    if(reason == 'find_successor'):
        query = ('find_successor',reason, key, value)
    if(reason == 'find_value'):
        query = ('find_value', reason, key, value)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sendr:
        addr = look_up(np)
        sendr.connect(addr)
        sendr.sendall(pickle.dumps(query))
        result = pickle.loads(sendr.recv(BUF_SZ))
    return result
    sendr.close()
                    
def getHash(key):
    """Method to generate hash value"""
    hashval = hashlib.sha1(pickle.dumps(key)).hexdigest()
    number = int(hashval, 16)
    return number

def look_up(node):
    """Method to look-up node"""
    return 'localhost', int(node) + TEST_BASE

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: Enter port number of the node to connect and key to search(eg. python chord_query.py 43549 tomfarris/25138611948)")
    else:
        port = int(sys.argv[1])
        key = str(sys.argv[2])
        port = port - TEST_BASE
        find_value_for_key(port, key)