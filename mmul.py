import sys
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint
import numpy as np
from random import random


##########################################################################
##########################################################################
# MapReduceSystem:

class MyMapReduce:
    __metaclass__ = ABCMeta

    def __init__(self, data, num_map_tasks=5, num_reduce_tasks=3, use_combiner=False):
        self.data = data  # the "file": list of all key value pairs
        self.num_map_tasks = num_map_tasks  # how many processes to spawn as map tasks
        self.num_reduce_tasks = num_reduce_tasks  # " " " as reduce tasks
        self.use_combiner = use_combiner  # whether or not to use a combiner within map task

    ###########################################################
    # programmer methods (to be overridden by inheriting class)

    @abstractmethod
    def map(self, k, v):
        print("Need to override map")

    @abstractmethod
    def reduce(self, k, vs):
        print("Need to overrirde reduce")

    ###########################################################
    # System Code: What the map reduce backend handles

    def mapTask(self, data_chunk, namenode_m2r, combiner=False):
        # runs the mappers on each record within the data_chunk and assigns each k,v to a reduce task
        mapped_kvs = []  # stored keys and values resulting from a map
        #print(f'Data {data_chunk}')
        for (k, v) in data_chunk:
            # run mappers:
            #print(f'Calling map with - {k} {v}')
            chunk_kvs = self.map(k, v)  # the resulting keys and values after running the map task
            mapped_kvs.extend(chunk_kvs) #A appends k,v pairs to final kvs result
            #pprint(f'Mapped Keys - {mapped_kvs}')
            # assign each kv pair to a reducer task
        pprint(f'Mapped{mapped_kvs}')
        if combiner:
            print (":")
        else:
            for (k, v) in mapped_kvs:
                #print(f'Creating RNODE')
                namenode_m2r.append((self.partitionFunction(k), (k, v)))
        #pprint(list(namenode_m2r))
        #print("\n")
        #print(f'END MAP')
    def partitionFunction(self, k): #A based on the key characters it allocates a node for processing
        # given a key returns the reduce task to send it
        node_number = np.sum([ord(c) for c in str(k)]) % self.num_reduce_tasks
        return node_number

    def reduceTask(self, kvs, namenode_fromR):
        # sort all values for each key (can use a list of dictionary)
        vsPerK = dict()
        for (k, v) in kvs:
            try:
                vsPerK[k].append(v)
            except KeyError:
                vsPerK[k] = [v]

        # call reducers on each key with a list of values
        # and append the result for each key to namenoe_fromR
        for k, vs in vsPerK.items():
            if vs:
                fromR = self.reduce(k, vs)
                if fromR:  # skip if reducer returns nothing (no data to pass along)
                    namenode_fromR.append(fromR)

    def runSystem(self):
        # runs the full map-reduce system processes on mrObject

        # [SEGMENT 1]
        # What: Declares two empty list types using the Manager class that is eventually going to store
        # the output of map and reduce steps.
        # Why: As the multiple processes need access to shared data for communication, we use the Message model to
        # create these lists to enable communication (read/write - write here) between processes.
        # the following two lists are shared by all processes
        # in order to simulate the communication
        namenode_m2r = Manager().list()  # stores the reducer task assignment and
        # each key-value pair returned from mappers
        # in the form: [(reduce_task_num, (k, v)), ...]
        # [COMBINER: when enabled this might hold]
        namenode_fromR = Manager().list()  # stores key-value pairs returned from reducers
        # in the form [(k, v), ...]

        # [SEGMENT 2]
        # What: It computes the chunk size that each map step can handle and divides the incoming data based on this chunk size
        # and creates child process for each of the map core available to start the map step.
        # Why: This is necessary to achieve parallelism among multiple resources available to process large set of data in quick time.
        #
        # divide up the data into chunks accord to num_map_tasks, launch a new process
        # for each map task, passing the chunk of data to it.
        # the following starts a process
        #      p = Process(target=self.mapTask, args=(chunk,namenode_m2r))
        #      p.start()
        processes = []
        chunkSize = int(np.ceil(len(self.data) / int(self.num_map_tasks)))
        chunkStart = 0
        while chunkStart < len(self.data):
            chunkEnd = min(chunkStart + chunkSize, len(self.data))
            chunk = self.data[chunkStart:chunkEnd]
            # print(" starting map task on ", chunk) #debug
            #print(f'{chunk} END')
            processes.append(Process(target=self.mapTask, args=(chunk, namenode_m2r, self.use_combiner)))
            processes[-1].start()
            chunkStart = chunkEnd


        # [SEGMENT 3]
        # What: We are looping through all the processes created and waiting for each of the process to complete execution so that
        # results are available for further processing. Then it is pretty printing (one per line) the result in a sorted order.
        # Why: This is to ensure that all the child process completes execution before the main process starts next step. We need this
        # as we have to wait for the results from all the map cores so that we can start the reduce step.
        # join map task processes back
        for p in processes:
            p.join()
            #print(f'Length of map output {len(namenode_m2r)}')
            # print output from map tasks
        print("namenode_m2r after map tasks complete:")
        pprint(sorted(list(namenode_m2r)))

        ##[SEGMENT 4]
        # What: This segment creates an array of empty lists based on the number of reducer cores available. Then based on the reducer
        # assignment from the map step, each of the inner lists are populated with a list of key value pairs (word,count) based on reducer number assigned.
        # Why: This step is a preprocessing step to ensure effective delegation and distribution of the workload to each of the reducer.
        #
        # "send" each key-value pair to its assigned reducer by placing each
        # into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
        to_reduce_task = [[] for i in range(self.num_reduce_tasks)]
        for (rtask_num, kv) in namenode_m2r:
            to_reduce_task[rtask_num].append(kv)

        # [SEGMENT 5]
        # What: This segment extract each of the list elements inside the to_reduce_task and creates a process to start the
        # reduction process. All the key value pairs assigned to a particular reducer will not start the reduction process.
        # Why: This step is needed to parallelize the process of reduction to achieve data processing with comparitively less time.
        #
        # launch the reduce tasks as a new process for each.
        processes = []
        for kvs in to_reduce_task:
            processes.append(Process(target=self.reduceTask, args=(kvs, namenode_fromR)))
            processes[-1].start()

        # [SEGMENT 6]
        # What: We are looping through all the processes created and waiting for each of the process to complete execution so that
        # results are available for word count. Then it is pretty printing (one per line) the result in a sorted order.
        #
        # Why: This is to ensure that all the child process completes execution before the main process finishes execution after
        # printing the result. We need this as we have to wait for the results from all the reducer cores.
        #
        # join the reduce tasks back
        for p in processes:
            p.join()
        # print output from reducer tasks
        print("namenode_fromR after reduce tasks complete:")
        pprint(sorted(list(namenode_fromR)))

        # return all key-value pairs:
        return namenode_fromR

class MatrixMultMR(MyMapReduce):  # [TODO]
    def map(self, k, v):
        kvs = []
        row = int(k[1])
        col = int(k[2])
        matrix = k[0][:k[0].find(":")]
        f1 = k[0].find(":")
        f2 = k[0].find(",")
        f4 = k[0].find(":", f1 + 1)
        f3 = k[0].find(",", f2 + 1)
        i = int(k[0][f1 + 1:f2])
        j = int(k[0][f2 + 1:f4])
        kval = int(k[0][f3 + 1:])
        print(f'ItH {k[0]}')
        #print(f'i={i} k={k} mat={matrix}  j={j}  value={v} row={row} col={col} ')
        if(matrix=="A"):
            for loop in range(kval):
                kvs.append(((row+1,loop+1),(matrix,col+1,v)))
            #pprint(f'Output of Reduce step -> {kvs}')
            return kvs;
        else:
            for loop in range(i):
                kvs.append(((loop+1, col+1), (matrix, row+1, v)))
            #pprint(f'Output of Reduce step -> {kvs}')
            return kvs;

    def reduce(self, k, vs):
        #print(f'RREDD {k} {vs}')#RREDD (1, 1) [('A', 1, 1), ('A', 2, 2), ('B', 1, 1), ('B', 2, 2)]
        def sortByJ(val):
            return val[1];

        A = []
        B = []
        for v in vs:
            if (v[0] == 'A'):
                A.append(v)
            else:
                B.append(v)
        A.sort(key=sortByJ)
        B.sort(key=sortByJ)
        sum = 0;
        for a, b in zip(A, B):
            sum = sum + a[2] * b[2];
        return (('R',k[0]-1,k[1]-1), sum)


##########################################################################
##########################################################################

from scipy import sparse


def createSparseMatrix(X, label):
    sparseX = sparse.coo_matrix(X)
    list = []
    for i, j, v in zip(sparseX.row, sparseX.col, sparseX.data):
        list.append(((label, i, j), v))
    return list


if __name__ == "__main__":


    ###################
    ##run Matrix Multiply:

    print("\n\n*****************\n Matrix Multiply\n*****************\n")
    #format: 'A|B:A.size:B.size
    #test1 = [(('A:1,2:2,1', 0, 0), 2.0), (('A:1,2:2,1', 0, 1), 1.0), (('B:1,2:2,1', 0, 0), 1), (('B:1,2:2,1', 1, 0), 3)]

    test1 = [(('A:2,2:2,1', 0, 0), 1), (('A:2,2:2,1', 0, 1), 2),(('A:2,2:2,1', 1, 0), 2),(('A:2,2:2,1', 1, 1), 1), (('B:2,2:2,1', 0, 0), 1),(('B:2,2:2,1', 1, 0), 2)]
    #test2 = createSparseMatrix([[1, 2, 4], [4, 8, 16]], 'A:2,3:3,3') + createSparseMatrix(
     #   [[1, 1, 1], [2, 2, 2], [4, 4, 4]], 'B:2,3:3,3')

    mrObject = MatrixMultMR(test1, 4, 3)
    mrObject.runSystem()
