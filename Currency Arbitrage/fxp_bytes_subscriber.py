# -*- coding: utf-8 -*-
"""
Created on Wed Oct 23 12:31:24 2019

@author: Nishigandha Mhatre
Seattle University
"""
import socket
from array import array
import selectors
from datetime import datetime, timedelta
import math
from collections import defaultdict
from bellman_ford import bellman_ford

server_address = ('cs2.seattleu.edu', 50403)  # Address of the Provider
liveForexData = {}                            # Ditionary to store the live feed
BUZ_FEED = 4096                               # Buffer size
SUBSCRIPTION_TIME = 10 * 60                   # Subscription time in seconds
MONEY_INVESTMENT = 100.00                     # $ 100 investment for the currency conversion profit

class fxp_bytes_subscriber(object):

    def __init__(self):
        self.selector = selectors.DefaultSelector()
        self._started_at = datetime.utcnow()
        self.listener, self.listener_addr = self.start_server()
        self.selector.register(self.listener, selectors.EVENT_READ)
        self.bellmanFord = bellman_ford()

    def serialize_address(self, ip_port : (str,int)) -> (bytes):
        """
        Method to convert address into bytes
        """
        address = bytes()
        ip_address , port = self.listener_addr
        ip_as_bytes = bytes(map(int, ip_address.split('.'))) #convert ip to bytes
        address += ip_as_bytes
        a = array('H', [port])
        a.byteswap() # convert to big-endian
        a.tobytes()
        address += a
        return address

    def start_server(self):
        """
        Method to create listening server
        """
        listener = socket.socket(socket.AF_INET , socket.SOCK_DGRAM)
        listener.bind(('localhost',0))
        listener_addr = listener.getsockname()
        return listener, listener_addr

    def run_forever(self, InComingData):
        """
        Method to keep listening for data
        """
        #print('waiting for data on {}'.format(self.listener_addr))
        next_timeout = 0.2  # FIXME
        while InComingData:
            events = self.selector.select(next_timeout)
            for key, mask in events:
                if mask & selectors.EVENT_READ:
                    data = self.read_data()
                    if data:
                        graph = self.generate_graph()
                        self.bellmanFord.findArbitartion(graph,'USD', MONEY_INVESTMENT)
                    else:
                        print("No data received")
            InComingData = self.check_subscribtion_expired()

    def read_data(self):
        """
        Method to accept data from the Publisher
        """
        try:
            
            data, _address = self.listener.recvfrom(BUZ_FEED)
            #print("Received {} bytes". format(len(data)))
            
            start = 0
            end = 32
            remaining = len(data)
            while remaining > 0 :
                self.deserialize_data(data[start:end])
                start = end
                end = start + 32
                remaining = remaining - 32
            self.listener.settimeout(5)
        except socket.error as err:
            print("Socket Failure %s"%(err))
            self.listener.close()

        return data

    def generate_graph(self):
        """
        Method to generate graph from the recived live data
        @return graph
        """
        graph = defaultdict(dict)
        for key , values in liveForexData.items():
            cur1, cur2 = key.split(" ")
            time , curRate = values
            graph[cur1][cur2] = float(-math.log(float(curRate)))
            graph[cur2][cur1] = float(math.log(float(curRate)))
        return graph

    def deserialize_data(self,data):
        """
        Method to deserialize the quote
        @param data in bytes
        """
        currencyToCurrency = self.getCurreny(data[8:14])
        microSeconds = self.getMicroSeconds(data[0:8])
        convRate = self.getConvRate(data[14:22])
        # check if exists whtr to override or keep
        convertToUtcDate = self.convertToUtcDate(microSeconds)
        if (currencyToCurrency in liveForexData):
            microSecondsOld, convRateOld = liveForexData[currencyToCurrency]
            if(microSecondsOld < microSeconds):
                print("removing stale quote for", currencyToCurrency)
                print(convertToUtcDate, currencyToCurrency, convRate)
                liveForexData[currencyToCurrency] = (microSeconds, convRate)
            else:
                print(convertToUtcDate , currencyToCurrency, convRate)
                print("ignoring out-of-sequence message")
        else :
            liveForexData[currencyToCurrency] = (microSeconds, convRate)
            print(convertToUtcDate, currencyToCurrency, convRate)

    def getConvRate(self, convRateInBytes):
        """
        Method to convert rate from bytes to float
        @param convRateInBytes
        @return conversion rate in float
        """
        convRate = array('d')
        convRate.frombytes(convRateInBytes)
        return convRate[0]

    def getCurreny(self, cTocInBytes):
        """
        Method to convert Currency from bytes to String
        @param curency in bytes
        @return currency in string
        """
        currencyToCurrencyInStr = cTocInBytes.decode('ascii')
        c1 = currencyToCurrencyInStr[0:3]
        c2 = currencyToCurrencyInStr[3:6]
        return c1+ " " + c2

    def getMicroSeconds(self, microSecondsInBytes):
       """
       Method to convert timestamp from bytes to time
       @param Timestamp in bytes
       @retun timestamp
       """
       timedata = array('Q')
       timedata.frombytes(microSecondsInBytes)
       timedata.byteswap()
       return timedata[0]

    def convertToUtcDate(self, microSeconds):
       """
       Method to convert timestamp into string
       """
       return str(datetime(1970, 1, 1) + timedelta(microseconds=microSeconds))

    
    def check_subscribtion_expired(self):
        """
        Method to check if the subscription has expired
        """
        time_passed = datetime.utcnow() - self._started_at
        if time_passed.total_seconds() > SUBSCRIPTION_TIME :
#            print("Renewing Subscription")
#            self.subscribe_renew()
#            return True
            #uncomment above 3 line and comment bleow 4 lines
            # if the program is needed to to renew subscription
            print("Subscribtion expired...")
            self.selector.unregister(self.listener)
            self.listener.close()
            return False
        return True
            
    def subscribe_renew(self):
        """
        Method to subscribe or renew subscribtion
        """
        with socket.socket(socket.AF_INET , socket.SOCK_DGRAM) as conn:
            conn.connect (server_address)
            conn.send(self.serialize_address(self.listener_addr))
        self._started_at = datetime.utcnow()


    