from collections import defaultdict, namedtuple, deque
from functools import partial
from math import floor

import os
import heapq
import random
import sys
import threading
import time

timeit = time.clock if sys.platform == 'win32' else time.time

DEBUG = False

def frange(start, stop, step):
    while start <= round(stop - step, 6):
        yield start
        start += step
        start = round(start, 6)

class Edge(object):
    def __init__(self, v, w, pdelay, capacity, nVC):
        assert(v < w)
        self.v = v
        self.w = w
        self.pdelay = pdelay
        self.capacity = capacity
        self.nVC = nVC
    def __str__(self):
        return 'pdelay: {:3d} capacity: {:3d} nVC: {:3d} load: {:.2f}'.format(self.pdelay, self.capacity, self.nVC, self.nVC/self.capacity*100)

class RoutingStatistic(object):
    def __init__(self, scheme):
        self.NumBlocked = 0
        self.TotalVCR = 0
        self.TotalPackets = 0
        self.NumSucPackets = 0
        self.PerSucPackets = 0
        self.NumBlkPackets = 0
        self.PerBlkPackets = 0
        self.NumHops = 0
        self.NumSucVC = 0
        self.S2D_PropDelay = 0
        self.scheme = scheme.upper()
        assert(self.scheme in ['CIRCUIT', 'PACKET'])


    def increase_TotalPackets(self, duration, packet_rate):
        if self.scheme == 'CIRCUIT':
            self.TotalPackets += floor(duration * packet_rate)
        else:
            self.TotalPackets += 1

    def increase_NumBlk(self, duration, packet_rate):
        self.NumBlocked += 1
        if self.scheme == 'CIRCUIT':
            self.NumBlkPackets += floor(packet_rate * duration)
        else:
            self.NumBlkPackets += 1

    def increase_NumSuc(self, packet_rate, duration, path_len, delay):
        self.NumSucVC += 1
        if self.scheme == 'CIRCUIT':
            self.NumSucPackets += floor(packet_rate * duration)
        else:
            self.NumSucPackets += 1

        self.NumHops += path_len
        self.S2D_PropDelay += delay

    def final(self):
        # print(self.NumHops, self.NumSucVC)
        self.PerSucPackets = self.NumSucPackets / self.TotalPackets if self.TotalPackets != 0 else 0
        self.PerBlkPackets = self.NumBlkPackets / self.TotalPackets if self.TotalPackets != 0 else 0
        self.NumHops = self.NumHops / self.NumSucVC  if self.NumSucVC != 0 else 0
        self.S2D_PropDelay = self.S2D_PropDelay / self.NumSucVC if self.NumSucVC != 0 else 0
        self.PerSucVC = self.NumSucVC / self.TotalVCR if self.TotalVCR != 0 else 0

    def __str__(self):
        return """total number of virtual circuit requests: {}
total number of packets: {}
number of successfully routed packets: {}
percentage of successfully routed packets: {:.2f}
number of blocked packets: {}
percentage of blocked packets: {:.2f}
average number of hops per circuit: {:.2f}
average cumulative propagation delay per circuit: {:.2f}
""".format(self.TotalVCR, self.TotalPackets, self.NumSucPackets, self.PerSucPackets * 100, self.NumBlkPackets, self.PerBlkPackets * 100, self.NumHops, self.S2D_PropDelay)


class WGraph(object):
    def __init__(self, NETWORK_SCHEME, ROUTING_SCHEME, TOPOLOGY_FILE,WORKLOAD_FILE, PACKET_RATE):

        self.param_check(NETWORK_SCHEME, ROUTING_SCHEME, TOPOLOGY_FILE,WORKLOAD_FILE, PACKET_RATE)
        self.generateGraph(self.topology_file)

        self.static_route = None
        self.generateStaticPath()

        self.statistic = RoutingStatistic(NETWORK_SCHEME)

        self.DecCapacity = partial(self.ChangeCapacity, val= 1)
        self.IncCapacity = partial(self.ChangeCapacity, val=-1)


    def param_check(self, NETWORK_SCHEME, ROUTING_SCHEME, TOPOLOGY_FILE,WORKLOAD_FILE, PACKET_RATE):
        if (NETWORK_SCHEME.upper() not in ['CIRCUIT', 'PACKET']):
            print('Invalid network scheme. [circuit, packet]')
            sys.exit()
        self.network_scheme = NETWORK_SCHEME.upper()
        if (ROUTING_SCHEME.upper() not in ['SHP', 'SDP', 'LLP']):
            print('Invalid routing scheme. [SHP, SDP, LLP]')
            sys.exit()
        self.routing_scheme = ROUTING_SCHEME.upper()
        if not (os.path.exists(TOPOLOGY_FILE)):
            print('{} does not exist'.format(TOPOLOGY_FILE))
            sys.exit()
        self.topology_file = TOPOLOGY_FILE
        if not (os.path.exists(WORKLOAD_FILE)):
            print('{} does not exist'.format(WORKLOAD_FILE))
            sys.exit()
        self.workload = WORKLOAD_FILE

        try:
            self.packet_rate = float(PACKET_RATE)
            if self.packet_rate <= 0:
                raise ValueError
        except ValueError:
            print('Invalid packet rate. packet rate > 0')
            sys.exit()
        self.packet_period = round(1/self.packet_rate, 6)

    @staticmethod
    def adjust(v, w):
        return (w, v) if v > w else (v, w)

    def addEdge(self, v, w, pdelay, capacity):
        assert(0 < pdelay < 200 and 0 < capacity < 100)
        if not self.adjacent(v, w):
            v, w = WGraph.adjust(v, w)
            self.edges[v].add(w)
            self.edges[w].add(v)
            self.weights[(v,w)] = Edge(v, w, pdelay, capacity, 0)
            self.nE += 1

    def getWeight(self, v, w):
        return self.weights[WGraph.adjust(v, w)]

    def avaiable(self, v, w):
        v, w = WGraph.adjust(v, w)
        return self.weights[(v, w)].capacity > self.weights[(v, w)].nVC

    def adjacent(self, v, w):
        return w in self.edges[v]

    def generateGraph(self, topo_file):
        self.nE = 0
        self.edges = defaultdict(set)
        self.weights = {}
        with open(topo_file) as fobj:
            for line in fobj:
                v, w, pdelay, capacity = line.split()
                assert(v != w)
                self.addEdge(v, w, int(pdelay), int(capacity))
        assert(all(x != {} for x in self.edges.values()))
        self.nV = len(self.edges)
        assert(self.dfs_checkValid())

    def _dfs_checkValid(self, v, visited):
        assert(not visited[v])
        visited[v] = True
        for w in self.edges[v]:
            if not visited[w]:
                self._dfs_checkValid(w, visited)

    def dfs_checkValid(self):
        visited = {x:False for x in self.edges}
        v = list(self.edges)[0]
        self._dfs_checkValid(v, visited)
        return all(x for x in visited.values())

    def printGraph(self):
        # for k, v in sorted(self.edges.items()):
            # print(k, ':', sorted(v))
        for k, v in self.weights.items():
            print(k, ':', v)

    def ChangeCapacity(self, v, w, val=None):
        v, w = WGraph.adjust(v, w)
        c, n = self.weights[(v, w)].capacity, self.weights[(v, w)].nVC
        self.weights[(v, w)].nVC = self.weights[(v, w)].nVC + val
        assert(self.weights[(v, w)].capacity >= self.weights[(v, w)].nVC)
        assert(self.weights[(v, w)].nVC >= 0)

    def getSHP_Weight(self, v, w):
        return 1

    def getSDP_Weight(self, v, w):
        return self.getWeight(v, w).pdelay

    def getLLP_Weight(self, v, w):
        temp = self.getWeight(v, w)
        return round((temp.nVC / temp.capacity), 6)


    @staticmethod
    def AddToHeap(heap, val, dist):
        for i in range(len(heap)):
            if heap[i][1] == val:
                heap[i][0] = dist[val]
                heapq.heapify(heap)
                return
        heapq.heappush(heap, [dist[val], val])

    def SSSP(self, v):
        getWeight = {'SHP' : self.getSHP_Weight, \
                     'SDP' : self.getSDP_Weight, \
                     'LLP' : self.getLLP_Weight}
        SumStragy = {'SHP' : sum, \
                     'SDP' : sum, \
                     'LLP' : max}

        MinDist = {x:float('Inf') for x in self.edges}
        MinDist[v] = 0

        marked = {x:False for x in self.edges}
        From = {x:[] for x in self.edges}
        From[v].append(v)
        minHeap = []
        heapq.heappush(minHeap, [MinDist[v], v])

        while minHeap:
            dis, curR = heapq.heappop(minHeap)
            if marked[curR]:
                continue
            marked[curR] = True
            for adjR in self.edges[curR]:
                if not marked[adjR]:
                    wt = getWeight[self.routing_scheme](curR, adjR)
                    new_dist = SumStragy[self.routing_scheme]([MinDist[curR], wt])
                    if (not From[adjR]) or (new_dist < MinDist[adjR]):
                        MinDist[adjR] = new_dist
                        From[adjR] = [curR]
                        WGraph.AddToHeap(minHeap, adjR, MinDist)
                    elif new_dist == MinDist[adjR]:
                        From[adjR].append(curR)
        return (From, MinDist)

    @staticmethod
    def GeneratePathes(From, dst, src):
        stack = deque([(dst, [dst])])
        result = []
        while stack:
            node, path = stack.pop()
            if node == src:
                result.append(path)
            for w in From[node]:
                if w not in path:
                    stack.appendleft((w, path + [w]))
        pathes = [x[::-1] for x in result]
        return [[(x[i], x[i+1]) for i in range(len(x)-1)] for x in pathes]


    def generateStaticPath(self):
        if self.routing_scheme == 'LLP':
            return
        self.static_route = {}
        for v in self.edges:
            self.static_route[v] = self.SSSP(v)


    def getWorkLoads(self):
        workloads = []
        # temp = 0
        with open(self.workload) as fobj:
            count = 0
            for line in fobj:
                self.statistic.TotalVCR += 1
                start, v, w, duration = line.split()
                start, duration = round(float(start), 6), round(float(duration), 6)
                # temp += floor(duration * self.packet_rate)
                if self.network_scheme == 'CIRCUIT':
                    count += 1
                    workloads.append((start, v, w, duration, 'S',count))
                    workloads.append((round(start+duration, 6), v, w, duration, 'E', count))
                else:
                    for i in frange(start, start+duration, self.packet_period):
                        count += 1
                        workloads.append((i, v, w, self.packet_period, 'S',count))
                        workloads.append((round(i+self.packet_period, 6), v, w, self.packet_period, 'E', count))
        # print(temp)
        return sorted(workloads)

    def runWorkLoads(self, workloads):
        started_wl = {}
        for tp, v, w, duration, com, wid in workloads:
            # print(tp, v, w, duration, com, wid);
            if com == 'S':
                path = self.EstVC(v, w)
                # if not path:
                #     print()
                # else:
                #     print(path)
                self.statistic.increase_TotalPackets(duration, self.packet_rate)
                if not path:
                    self.statistic.increase_NumBlk(duration, self.packet_rate)
                else:
                    path_delay = sum(self.getWeight(*x).pdelay for x in path)
                    self.statistic.increase_NumSuc(self.packet_rate, duration, len(path), path_delay)
                    self.acquirePath(path)
                    assert((tp, v, w, wid) not in started_wl)
                    started_wl[(v, w, wid)] = path
            elif (v, w, wid) in started_wl:
                self.releasePath(started_wl.pop((v, w, wid)))


    def StartRouting(self):
        workloads = self.getWorkLoads()
        self.runWorkLoads(workloads)
        self.statistic.final()
        print(self.statistic)

    def acquirePath(self, path):
        for link in path:
            self.DecCapacity(*link)

    def releasePath(self, path):
        for link in path:
            self.IncCapacity(*link)


    def GenerateCongLink(self, path):
        cong_link = set()
        for link in path:
            cong_link.add(link)
        cong_link = set(x for x in cong_link if not self.avaiable(*x))
        return cong_link


    def EstVC(self, v, w):
        From, MinDist = self.SSSP(v) if self.routing_scheme == 'LLP' else self.static_route[v]
        pathes = WGraph.GeneratePathes(From, w, v)
        selected_path = random.sample(pathes, 1)[0]
        # print(len(pathes), pathes.index(selected_path))
        cong_link = self.GenerateCongLink(selected_path)
        return selected_path if all(x not in cong_link for x in selected_path) else None

t1 = time.time()
NETWORK_SCHEME =  'PACKET' #'CIRCUIT' # or 'PACKET'
ROUTING_SCHEME = 'LLP' # 'SHP' 'SDP' 'LLP'
TOPOLOGY_FILE = 'topology.txt'
WORKLOAD_FILE = 'workload.txt'
PACKET_RATE = '2'
print(WORKLOAD_FILE.split('/')[-1] ,NETWORK_SCHEME, ROUTING_SCHEME, PACKET_RATE)

router_info = WGraph(NETWORK_SCHEME, ROUTING_SCHEME, TOPOLOGY_FILE,WORKLOAD_FILE, PACKET_RATE)
router_info.StartRouting()
print(time.time() - t1)
# if len(sys.argv) != 6:
#     print('Usage: python3 RoutingPerformance NETWORK_SCHEME ROUTING_SCHEME TOPOLOGY_FILE WORKLOAD_FILE PACKET_RATE')
# else:
#     NETWORK_SCHEME = sys.argv[1]
#     ROUTING_SCHEME = sys.argv[2]
#     TOPOLOGY_FILE = sys.argv[3]
#     WORKLOAD_FILE = sys.argv[4]
#     PACKET_RATE = sys.argv[5]

#     router_info = WGraph(NETWORK_SCHEME, ROUTING_SCHEME, TOPOLOGY_FILE,WORKLOAD_FILE, PACKET_RATE)
#     router_info.StartRouting()
