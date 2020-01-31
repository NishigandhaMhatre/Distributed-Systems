# -*- coding: utf-8 -*-
"""
Created on Sun Sep 29 19:21:38 2019

@author: Nishigandha Mhatre
Seattle University
"""

# Echo client program
import socket
import sys
import pickle

class ClientProgram:
    """
    Client Program to connect to Group Coordinator Daemon which returns list of member nodes
    """
    
    def __init__(self,host,port):
        self.host=host
        self.port=port
        self.recvlist = self.connectServer()
        self.connectMember(self.recvlist)
        
    
    def connectServer(self):
        """
        Function to connect to Group Coordinator Daemon
        Returns list of member nodes
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((self.host, self.port))
                s.settimeout(1500)
                raw = pickle.dumps('JOIN')
                print('JOIN ('+ str(self.host) + ',' + str(self.port) + ')')
                s.sendall(raw)
                recvData = s.recv(1024) 
                recvlist = pickle.loads(recvData)
            except (socket.error, socket.timeout) as err:
                print("Socket created failed: %s"%(err))
        return recvlist
    
        
    def connectMember(self,recvlist):
        """
        Function to send message to member nodes
        Accepts the list of host and port of member nodes 
        """
        for hostPort in recvlist:
            host1 = hostPort.get('host')
            port1 = hostPort.get('port')
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.connect((host1,port1))
                    s.settimeout(1500)
                    memberMsg = pickle.dumps('HELLO')
                    s.sendall(memberMsg)
                    recvMsgRaw = s.recv(1024)
                    recvMsg = pickle.loads(recvMsgRaw)
                    print(recvMsg)
                except (socket.error, socket.timeout) as err:
                    print("Failed to connect"+" {"+str(host1)+" , "+str(port1)+"} %s"%(err))
                    s.close()
                    s = None
                    continue
    
    
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python client.py HOST PORT")
        exit(1)
    else:
        host = sys.argv[1]
        port = int(sys.argv[2])
        ClientProgram(host,port)
    
        