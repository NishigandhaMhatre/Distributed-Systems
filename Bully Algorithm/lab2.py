# -*- coding: utf-8 -*-
"""
Created on Tue Oct  8 20:23:00 2019
CPSC 5520, Seattle University
@author: Nishigandha Mhatre
Program to implement Bully Algorithm
"""
import sys
import socket
from datetime import datetime 
from dateutil.parser import parse
import pickle
import selectors
import enum

CHECK_INTERVAL = 2.0
PEER_DIGITS = 100
ASSUME_FAILURE_TIMEOUT = 2.0

class State(enum.Enum):
    """
    Enumeration of states a peer can be in for the Lab2 class.
    """
    QUIESCENT = 'QUIESCENT'  # Erase any memory of this peer

    # Outgoing message is pending
    SEND_ELECTION = 'ELECTION'
    SEND_VICTORY = 'COORDINATOR'
    SEND_OK = 'OK'

    # Incoming message is pending
    WAITING_FOR_OK = 'WAIT_OK'  # When I've sent them an ELECTION message
    WAITING_FOR_VICTOR = 'WHO IS THE WINNER?'  # This one only applies to myself
    WAITING_FOR_ANY_MESSAGE = 'WAITING'  # When I've done an accept on their connect to my server
    

    
class lab2:
    
    def __init__(self, gcd_address, next_birthday, su_id):
        self.gcd_address = (gcd_address[0], int(gcd_address[1]))
        days_to_birthday = (next_birthday - datetime.now()).days   
        self.pid = (days_to_birthday,int(su_id))
        self.bully = None
        self.members = {}
        self.states = {}
        self.selector = selectors.DefaultSelector()
        self.listener , self.listener_addr = self.start_a_server()
        
    def run(self):
        """
        Function to poll the sockets
        """
        #self.selector.register(self.listener, selectors.EVENT_READ, data = None)
        while True:
            events = self.selector.select(CHECK_INTERVAL)
            for key, mask in events:
                if key.fileobj == self.listener:
                    self.accept_peer()
                elif mask & selectors.EVENT_READ:
                    self.receive_message(key.fileobj)
                else:
                    self.send_message(key.fileobj)
            self.check_timeouts()
            
    def check_timeouts(self):
        """
        Function to check the socket timeouts
        """
        self.listener.settimeout(ASSUME_FAILURE_TIMEOUT)
        
        
    def start_a_server(self):
        """
        Method to create listening server. 
        """
        listener = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        listener.bind(('localhost',0))
        listener.listen(5)
        listener.setblocking(False)
        self.selector.register(listener, selectors.EVENT_READ, data = None)
        listener_addr = listener.getsockname()
        return listener , listener_addr
    
    def accept_peer(self):
        """
        Method to accept connection 
        """
        conn, addr = self.listener.accept()  # Should be ready
        print('accepted connection from',addr)
        conn.setblocking(False)
        self.selector.register(conn, selectors.EVENT_READ, data=None)
    
    def join_group(self):
        """
        Method to connect to the GCD to join the group
        """
        recvList = []
        try:
            print("Entered connect_GCD")
            s=self.get_connection(self.gcd_address)
            message_name = 'JOIN'
            print(self.listener_addr)
            message_data = (self.pid,self.listener_addr)
            self.send(s,message_name, message_data, True)
            recvData = s.recv(4098)
            recvList = pickle.loads(recvData)
            print("GCD sent: ",recvList)
        except (socket.error, socket.timeout) as err:
            print("Socket creation failed: %s"%(err))
        return recvList    
    
    def start_election(self,reason):
        """
        Election Algorithm
        """
        i_am_biggest_bully = True
        if reason:
            myself_daysToBday , myself_suid = self.pid 
            self.states[self.listener] = 'ELECTION_IN_PROGRESS'
            for peer_pid , peer_addr in self.members.items():
                peer_daysToBday , peer_suid = peer_pid
                if(peer_daysToBday > myself_daysToBday):
                    i_am_biggest_bully = False
                    peer = self.get_connection(peer_addr)
                    self.states[peer] = State.SEND_ELECTION
                    self.send_message(peer)
                elif (peer_daysToBday == myself_daysToBday):
                    if(peer_suid > myself_suid):
                       i_am_biggest_bully = False
                       peer = self.get_connection(peer_addr) 
                       self.states[peer] = State.SEND_ELECTION
                       self.send_message(peer)
                else:
                    i_am_biggest_bully = True
                    reason = "I am the highest prioity process"
                
                if(self.states[self.listener] == State.QUIESCENT):
                       i_am_biggest_bully = False 
                       break
                else:
                    i_am_biggest_bully = True
                    reason = 'OK not received'
        if(i_am_biggest_bully == True):
            self.set_quiescent()
            self.bully = self.pid
            self.declare_victory(reason)
                    
                    
     
    def update_members(self , recvMemberList):
        """
        Function to update the member list
        """
        for key, value in recvMemberList.items():
            self.members[key] = value 
        
        
    def send_message(self, peer):
        """
        Function to send message based on the State
        """
        state = self.get_state(peer)
        try:
            
            print('{}: sending {} [{}]'.format(self.pr_sock(peer) , state , self.pr_now()))
            try:
                self.send(peer, state.value, self.members)
            except ConnectionError as err:
                print("Connection error %s"%(err))
        except Exception as err:
            print("Socket creation failed %s"%(err))
        if state == State.SEND_ELECTION:
            self.set_state(State.WAITING_FOR_OK, peer, switch_mode=True)
        else:
            self.set_quiescent(peer)
            
    def send(cls, peer ,message_name , message_data= None, wait_for_repy = False):
        """
        Method to send Message
        """
        message = (message_name, message_data)
        message = pickle.dumps(message)
        peer.sendall(message)
        if(wait_for_repy == False):
            cls.selector.unregister(peer)
            peer.close()

    
    def receive_message(self, peer):
        """
        Method to receive data
        """
        try:
            recvData = peer.recv(4098)
            recvDict = pickle.loads(recvData)
            if recvDict:
                message_name , message_data = recvDict
                print("Received: ",message_name)
                self.update_members(message_data)
                if(message_name == 'ELECTION'):
                    if not self.is_election_in_progress():
                        self.states[self.listener] = 'ELECTION_IN_PROGRESS'
                        self.set_state(State.SEND_OK,peer)
                        self.send_message(peer)
                        self.start_election('ELECTION')
                elif (message_name == 'OK'):
                    if(self.states[peer] == State.WAITING_FOR_OK):
                        self.set_state(State.QUIESCENT,peer)
                        self.set_quiescent()
                elif (message_name == 'COORDINATOR'):
                    self.set_quiescent()
                    self.bully = peer.getsockname()
            else:
                self.selector.unregister(peer)
                peer.close()
        except Exception:
            self.selector.unregister(peer)
            peer.close()
        
        
    def get_connection(self, member):
        """
        Method to create socket
        """
        listener = member
        peer = socket.socket(socket.AF_INET , socket.SOCK_STREAM) 
        peer.connect(listener)
        #peer.setblocking(False)
        self.selector.register(peer, selectors.EVENT_WRITE, data = None)
        return peer
        
    def get_state(self, peer=None):
        """
        Look up current state in state table.

        :param peer: socket connected to peer process (None means self)
        :param detail: if True, then the state and timestamp are both returned
        :return: either the state or (state, timestamp) depending on detail (not found gives (QUIESCENT, None))
        """
        if peer is None:
            peer = self
        status = self.states[peer] if peer in self.states else State.QUIESCENT
        return status
        
    def set_state(self, state , peer=None, switch_mode = False):
        """
        Method to update the state of socket
        """
        if switch_mode:
            self.states[self.listener] = state 
        else:
            self.states[peer] = state
            
    def is_election_in_progress(self):
        """
        Method to check if election is in progress
        """
        if self.states[self.listener] == 'ELECTION_IN_PROGRESS':
            return True
        else:
            return False
    
    def set_leader(self, new_leader):
        """
        Method to update the leader
        """
        self.bully = new_leader
    
    def set_quiescent(self, peer= None):
        """
        Method to update state to quiesent
        """
        self.set_state(State.QUIESCENT, peer, switch_mode = True)
    
    def declare_victory(self, reason):
        """
        Declare victory to peers
        """
        if(self.bully == self.pid):
            print("The Leader is: {} as {}".format(self.pr_leader(),reason))
            for member in self.members.items():
                pid , addr = member
                if self.listener_addr != addr:
                    peer = self.get_connection(addr)
                    self.states[peer] = State.SEND_VICTORY
                    self.send_message(peer)
            
    # Helper printing methods        
    @staticmethod
    def pr_now():
        return datetime.now().strftime('%H:%M:%S.%f')
    
    def pr_sock(self, sock):
        if sock is None or sock == self or sock == self.listener:
            return 'self'
        return self.cpr_sock(sock)
    
    @staticmethod
    def cpr_sock(sock):
        l_port = sock.getsockname()[1] % PEER_DIGITS
        try:
            r_port = sock.getsockname()[1] % PEER_DIGITS
        except OSError:
            r_port = '???'
        return '{}->{} ({})'.format(l_port,r_port, id(sock))
    
    def pr_leader(self):
        return 'unknown' if self.bully is None else ('self' if self.bully == self.pid else self.bully)
            
if __name__ == '__main__':
    """
    Commandline expected in below format: python lab2.py localhost 1234 2020-01-01 4061741
    """
    if len(sys.argv) != 5:
        print("Usage: python lab2.py GCD hostname port your birthday your SUID")
    else:
        gcd_address = (sys.argv[1], sys.argv[2])
        next_birthday = parse(sys.argv[3])
        su_id = int(sys.argv[4])
        myself = lab2(gcd_address, next_birthday, su_id)
        recvList=myself.join_group()
        myself.update_members(recvList)
        myself.states[myself.listener] = 'ELECTION_IN_PROGRESS'
        myself.start_election('JOIN')
        myself.run()
    
