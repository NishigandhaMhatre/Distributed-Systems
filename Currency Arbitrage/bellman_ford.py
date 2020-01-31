# -*- coding: utf-8 -*-
"""
Created on Wed Oct 23 12:31:24 2019

@author: Nishigandha Mhatre
Seattle University
"""
import math

TOLERANCE = 0.000000000001

class bellman_ford(object):
    """
    Bellman ford class to find the Arbitrage in currency
    """
    
    def initializeDictionaries(self,graph, source):
        """
        Method to initialize the distances with infinity and predecessor to None
        @return distanceFromNode, predecessorNode
        """
        distanceFromNode = {} # Dictionary to store shortest distance of each vertex from source
        predecessorNode = {}  # Dictionary to store the predecessor of every node
        for vertex in graph:
            distanceFromNode[vertex] = float("Inf")
            predecessorNode[vertex] = None
        distanceFromNode[source] = 0
        return distanceFromNode, predecessorNode
       
    
    def relaxEdges(self,vertex1, vertex2, graph, distanceFromNode,predecessorNode):
        """
        Helper method to relax edges 
        """
        if (distanceFromNode[vertex1]!= float("Inf") and distanceFromNode[vertex1] + graph[vertex1][vertex2] < distanceFromNode[vertex2] - TOLERANCE):
            distanceFromNode[vertex2] = distanceFromNode[vertex1] + graph[vertex1][vertex2]
            predecessorNode[vertex2] = vertex1
            
            
    def backtrack(self,predecessorNode, source):
        """
        Method to backtrack the path if negetive loop found
        @return Path with arbitrage
        """
        path = [source]
        nextVer = source
        while True:
            nextVer = predecessorNode[nextVer]
            if nextVer!= None:
                if(nextVer not in path):
                    path.append(nextVer)
                else:
                    path.append(nextVer)
                    temp = list(reversed(path[:path.index(nextVer)]))
                    path.extend(temp)
                    return list(reversed(path))
            else:
                return

    def getProfit(self, graph, source, path, initialInvestment):
        """
        Method to check if the profit is above 1 dollar
        """
        profit = initialInvestment
        for i,value in enumerate(path):
            if i+1 < len(path):
                start = path[i]
                end = path[i+1]
                rate = math.exp(-graph[start][end])
                profit = profit * rate
        if(profit > initialInvestment):
            return True
        else:
            return False

    def bellmanFord(self,graph, source, money):
        """
        Bellan ford algorithm. Finds arbritage paths if any which yield profit more than tolerance(1USD)
        """
        distanceFromNode, predecessorNode = self.initializeDictionaries(graph, source)
        for i in range(len(graph)-1):
            for ver1 in graph:
                for ver2 in graph[ver1]:
                    self.relaxEdges(ver1, ver2, graph, distanceFromNode, predecessorNode)
                    if(distanceFromNode[source] < 0 - TOLERANCE):
                        path = self.backtrack(predecessorNode, source)
                        if(self.getProfit(graph, source, path, money)):
                            return(path)
        for ver1 in graph:
                for ver2 in graph[ver1]:
                    if distanceFromNode[ver1]!= float("Inf") and distanceFromNode[ver2] < distanceFromNode[ver1] + graph[ver1][ver2]:
                        path = self.backtrack(predecessorNode, source)
                        if(self.getProfit(graph, source, path, money)):
                            return(path)
        return None



    def findArbitartion(self,graph, source, money):
        """
        Driver method to find and print arbritarage in currencies
        """
        path = self.bellmanFord(graph, 'USD', money)
        if path == None:
            return
        else:
            print("ARBITRAGE:")
            print("\tStart with %(currency)s %(money)i" % {"currency":path[0], "money":money})
            for i,value in enumerate(path):
                if i+1 < len(path):
                    start = path[i]
                    end = path[i+1]
                    rate = math.exp(-graph[start][end])
                    money = money * rate
                    print("\texchange %(start)s for %(end)s at %(rate)f --> %(end)s %(money)f" % {"start":start,"end":end,"rate":rate,"end":end, "money":money})
            print("")
    