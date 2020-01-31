# -*- coding: utf-8 -*-
"""
Created on Sat Nov  9 17:18:31 2019

@author: Nishigandha Mhatre
Seattle University
Assignment: Distrubuted hash table (chord_node.py)
"""
import socket
import sys
import pickle
import hashlib
import threading
from random import randint

M = 3  # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
NODES = 2**M
BUF_SZ = 4096 * 4096  # socket recv arg
BACKLOG = 100  # socket listen arg
TEST_BASE = 43544  # for testing use port numbers on localhost at TEST_BASE+n

class ModRange(object):

    def __init__(self, start, stop, divisor):
        self.divisor = divisor
        self.start = start % self.divisor
        self.stop = stop % self.divisor
        # we want to use ranges to make things speedy, but if it wraps around the 0 node, we have to use two
        if self.start < self.stop:
            self.intervals = (range(self.start, self.stop),)
        else:
            self.intervals = (range(self.start, self.divisor), range(0, self.stop))

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return '[{},{})'.format(self.start, self.stop, self.divisor)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        for interval in self.intervals:
            if id in interval:
                return True
        return False

    def __len__(self):
        total = 0
        for interval in self.intervals:
            total += len(interval)
        return total

    def __iter__(self):
        return ModRangeIter(self, 0, -1)


class ModRangeIter(object):
    """ Iterator class for ModRange """
    def __init__(self, mr, i, j):
        self.mr, self.i, self.j = mr, i, j

    def __iter__(self):
        return ModRangeIter(self.mr, self.i, self.j)

    def __next__(self):
        if self.j == len(self.mr.intervals[self.i]) - 1:
            if self.i == len(self.mr.intervals) - 1:
                raise StopIteration()
            else:
                self.i += 1
                self.j = 0
        else:
            self.j += 1
        return self.mr.intervals[self.i][self.j]

class FingerEntry(object):

    def __init__(self, n, k, node=None):
        if not (0 <= n < NODES and 0 < k <= M):
            raise ValueError('invalid finger entry values')
        self.start = (n + 2**(k-1)) % NODES
        self.next_start = (n + 2**k) % NODES if k < M else n
        self.interval = ModRange(self.start, self.next_start, NODES)
        self.node = node
   

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return 'start:{} Interval:{} Succ:{}'.format(self.start,self.interval, self.node)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        return id in self.interval
    
class ChordNode(object):
    """
    Chord node main class 
    """
    def __init__(self, n):
        self.node = n
        self.finger = [None] + [FingerEntry(n, k) for k in range(1, M+1)]  # indexing starts at 1
        self.predecessor = None
        self.keys = {}
        self.keyVal = {}
        
    @property
    def successor(self):
        return self.finger[1].node
   
    @successor.setter
    def successor(self, id):
        self.finger[1].node = id
       
    def update_predecessor(self,id):
        """ Method to update predecessor """
        self.predecessor = id
        return True
       
    def find_successor(self, id):
        """ Method to find successor """
        np = self.find_predecessor(id)
        return self.call_rpc(np, 'successor')

    def find_predecessor(self,id): 
        """ Method to find predecessor """
        np = self.node
        while id not in ModRange(np+1, self.call_rpc(np,'successor')+1, NODES):
            np = self.call_rpc(np,'find_closest_preceding_finger',id)
        return np

    def find_closest_preceding_finger(self,id):
        """ Method to find the closest preceding node"""
        for i in range(M,0,-1):
            if(self.finger[i].node in ModRange(self.node+1,id,NODES)): # verify
                return self.finger[i].node
        return self.node
       
    def joinChord(self, np):
        """ Method to join the chord system"""
        if(port != 0):
            np = np - TEST_BASE
            self.init_finger_table(np)
            self.updateOthers()
            result = self.call_rpc(np,'shift_keys')
            if result:
                self.keys ,self.keyVal = result
                print("Moved keys from successor: ",self.keys)
        else:
            for i in range(1, M+1):
                self.finger[i].node = self.node
            self.predecessor = self.node
            self.successor = self.node
            
            
    def init_finger_table(self,np):
        """ Method to initialize nodes finger table"""
        self.finger[1].node = self.call_rpc(np,'find_successor',self.finger[1].start)
        self.predecessor = self.call_rpc(self.finger[1].node,'predecessor') #add in call rpc
        self.call_rpc(self.finger[1].node,'update_predecessor',self.node)        # add in call rpc
        for i in range(1,M):
            if(self.finger[i+1].start in ModRange(self.node,self.finger[i].node,NODES)):
                self.finger[i+1].node = self.finger[i].node
            else:
                self.finger[i+1].node = self.call_rpc(np,'find_successor',self.finger[i+1].start)


    def updateOthers(self): # prof code
        """ Update all other node that should have this node in their finger tables """
        for i in range(1, M+1):  # find last node p whose i-th finger might be this node
            p = self.find_predecessor((1 + self.node - 2**(i-1) + NODES) % NODES)  # FIXME: bug in paper, 1+
            self.call_rpc(p, 'update_finger_table', self.node, i)
           
   
    def updateFingerTable(self, s, i): #prof code
        """ if s is i-th finger of n, update this node's finger table with s """
        if (self.finger[i].start != self.finger[i].node  # FIXME: don't want e.g. [1, 1) which is the whole circle
                and s in ModRange(self.finger[i].start, self.finger[i].node, NODES)):  # FIXME: bug in paper, [.start
            print('update_finger_table({},{}): {}[{}] = {} since {} in [{},{})'.format(s, i, self.node, i, s, s,
                                                                                       self.finger[i].start,
                                                                                       self.finger[i].node))
            self.finger[i].node = s
            print('#', self.finger)
            p = self.predecessor  # get first node preceding myself
            if(p!=s):
                self.call_rpc(p, 'update_finger_table', s, i) #add in rpc
            return str(self)
        return 'did nothing {}'.format(self)

    def call_rpc(self,np,reason = None, key = None,index=None):
        """Method to generate query for rpc call"""
        if(reason == 'successor'):
            if(np == self.node):
                return self.successor
            else:
                query = ('successor',reason,key,index)
        if(reason == 'find_successor'):
            query = ('find_successor',reason,key,index)
        if(reason == 'predecessor'):
            if(np == self.node):
                return self.predecessor
            else:
                query = ('predecessor',reason, key, index)
        if(reason == 'find_predecessor'):
            query = ('find_predecessor',reason,key,index)
        if(reason == 'find_closest_preceding_finger'):
            if (np == self.node):
                return self.find_closest_preceding_finger(key)
            else:
                query = ('find_closest_preceding_finger',reason,key,index)
        if(reason == 'update_predecessor'):
            if (np == self.node):
                self.predecessor = key
            else:
                query = ('update_predecessor',reason, key,index)
        if(reason == 'update_finger_table'):
            if(np == self.node):
                self.updateFingerTable(key,index)
                return
            else:
                query = ('updateFingerTable',reason,key,index)
        if(reason == 'shift_keys'):
            query = ('shift_keys', reason, key, index)
        with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as sendr:
            addr = ChordNode.lookup_node(np)
            sendr.connect(addr)
            sendr.sendall(pickle.dumps(query))
            recvResult = sendr.recv(BUF_SZ)
            result = pickle.loads(recvResult)
        return result

    def lookup_node(n):
        """Method to lookup port number"""
        return 'localhost',TEST_BASE + n  
   
    @staticmethod  
    def start_a_server(node):
        """Method to create listener server"""
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        addr = ChordNode.lookup_node(node)
        listener.bind(addr)  # use any free socket
        listener.listen(1)
        return listener , listener.getsockname()
   
    def listen_connect_request(self,server):
        """Method to listen incoming requests"""
        while True:
            client, client_addr = server.accept()
            threading.Thread(target=self.handle_rpc, args=(client,)).start()
   
   
    def handle_rpc(self,client):
        """Method to handle rpc"""
        rpc = client.recv(BUF_SZ)
        method, arg1, arg2, arg3 = pickle.loads(rpc)
        result = self.dispatch_rpc(method, arg1, arg2,arg3)
        client.sendall(pickle.dumps(result))
       
    def dispatch_rpc(self,method, reason, key,index):
        """Method to dispatch rpc requests"""
        if(reason == 'find_successor'):
            result = self.find_successor(key)
        elif(reason == 'successor'):
            result = self.successor
        elif(reason == 'find_predecessor'):
            result = self.find_predecessor(key)
        elif(reason == 'predecessor'):
            result = self.predecessor
        elif(reason == 'find_closest_preceding_finger'):
            result = self.find_closest_preceding_finger(key)
        elif(reason == 'update_predecessor'):
            result = self.update_predecessor(key)
        elif(reason == 'update_finger_table'):
            result = self.updateFingerTable(key,index)
        elif(reason == 'update_keys_values'):
            result = self.populate_keys(key, index)
        elif(reason == 'shift_keys'):
            result = self.shift_keys()
        elif (reason == 'find_value'):
            result = self.find_value(key)
        return result
   
    def getHash(key):
        """Method to generate hash value"""
        hashval = hashlib.sha1(pickle.dumps(key)).hexdigest()
        number = int(hashval, 16)
        return number
    
    def populate_keys(self, key, value):
        """Method to populate keys in node"""
        index = ChordNode.getHash(key) % NODES
        self.keys[key] = index
        self.keyVal[key] = value
        return True
        
    def find_value(self,key):
        """Method to return requested key"""
        return self.keyVal[key]
    
    def shift_keys(self):
        """Method to move keys when new node joins the network"""
        resultKeyVal={}
        resultKeys = {}
        if not self.keys:
            return False
        else:
            for key, index in self.keys.items(): #move keys to predecessors bucket
                if index not in ModRange(self.predecessor + 1, self.node+1, NODES):
                    resultKeys[key] = index
                    resultKeyVal[key] = self.keyVal[key]    
            tempKeys = {}
            tempKeyVal = {}
            for key, index in self.keys.items(): # delete moved keys
                if index in ModRange(self.predecessor + 1, self.node+1, NODES):
                    tempKeys[key] = index
                    tempKeyVal[key] = self.keyVal[key]
            self.keys = tempKeys
            self.keyVal = tempKeyVal
            return resultKeys, resultKeyVal
        
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: Enter 0 or port number of existing node (eg. python chord_node.py 43547)")
    else:
        port = int(sys.argv[1])
        identifier = ('localhost',randint(1,1000)) #using random generated value for port identifier
        node = ChordNode.getHash(identifier) % NODES
        print("Me: ",node)
        listener, identifier = ChordNode.start_a_server(node)
        print("Listening on.....{}".format(identifier))
        myself = ChordNode(node)
        myself.joinChord(port)
        print("\n*****************Printing Finger Table************************\n")
        for x in myself.finger:
           print(x)
      
        myself.listen_connect_request(listener)