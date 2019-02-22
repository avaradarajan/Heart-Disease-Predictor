##########################################################################
## MyMapReduceSystem.py  v 0.32
##
## Implements a basic version of MapReduce intended to run
## on multiple threads of a single system. This implementation
## is simply intended as an instructional tool for students
## to better understand what a MapReduce system is doing
## in the backend in order to better understand how to
## program effective mappers and reducers.
##
## MyMapReduce is meant to be inheritted by programs
## using it. See the example "WordCountMR" class for
## an exaample of how a map reduce programmer would
## use the MyMapReduce system by simply defining
## a map and a reduce method.
##
##
## Original Code written by H. Andrew Schwartz
## for SBU's Big Data Analytics Course
## version 0.32 - Spring 2019
##
## Student Name: Anandh Varadarajan
## Student ID: 112505082


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
        for (k, v) in data_chunk:
            # run mappers:
            chunk_kvs = self.map(k, v)  # the resulting keys and values after running the map task
            mapped_kvs.extend(chunk_kvs) #A appends k,v pairs to final kvs result

            # assign each kv pair to a reducer task
            if combiner:
                # [ADD COMBINER HERE]
                print("\nCombiner not implemented in this version.")  # remove this line
            else:
                for (k, v) in mapped_kvs:
                    namenode_m2r.append((self.partitionFunction(k), (k, v)))

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


##########################################################################
##########################################################################
##Map Reducers:

class WordCountMR(MyMapReduce):  # [DONE]
    # the mapper and reducer for word count
    def map(self, k, v):  # [DONE]
        counts = dict()
        for w in v.split():
            w = w.lower()  # makes this case-insensitive
            try:  # try/except KeyError is just a faster way to check if w is in counts:
                counts[w] += 1
            except KeyError:
                counts[w] = 1
        return counts.items()

    def reduce(self, k, vs):  # [DONE]
        return (k, np.sum(vs))


class WordCountBasicMR(MyMapReduce):  # [DONE]
    # mapper and reducer for a more basic word count
    # -- uses a mapper that does not do any counting itself
    def map(self, k, v):
        kvs = []
        counts = dict()
        for w in v.split():
            kvs.append((w.lower(), 1))
        return kvs

    def reduce(self, k, vs):
        return (k, np.sum(vs))


class SetDifferenceMR(MyMapReduce):
    # contains the map and reduce function for set difference
    # Assume that the mapper receives the "set" as a list of any primitives or comparable objects
    def map(self, k, v):
        toReturn = []
        for i in v:
            toReturn.append((i, k))
        return toReturn

    def reduce(self, k, vs):
        if len(vs) == 1 and vs[0] == 'R':
            return k
        else:
            return None


class MatrixMultMR(MyMapReduce):  # [TODO]
    def map(self, k, v):
        # <COMPLETE>
        return [(k, v)]

    def reduce(self, k, vs):
        # <COMPLETE>
        return (k, vs)


##########################################################################
##########################################################################

from scipy import sparse


def createSparseMatrix(X, label):
    sparseX = sparse.coo_matrix(X)
    list = []
    for i, j, v in zip(sparseX.row, sparseX.col, sparseX.data):
        list.append(((label, i, j), v))
    return list


if __name__ == "__main__":  # [Uncomment peices to test]

    ###################
    ##run WordCount:

    print("\n\n*****************\n Word Count\n*****************\n")
    data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
            (8,
             "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
            (9, "The car raced past the finish line just in time."),
            (10, "Car engines purred and the tires burned.")]
    print("\nWord Count Basic WITHOUT Combiner:")
    mrObjectNoCombiner = WordCountBasicMR(data, 4, 3)
    mrObjectNoCombiner.runSystem()
    '''
    print("\nWord Count Basic WITH Combiner:")
    mrObjectWCombiner = WordCountBasicMR(data, 4, 3, use_combiner=True)
    mrObjectWCombiner.runSystem()
    '''

    ####################
    ##run SetDifference (nothing to do here; just another test)
    print("\n\n*****************\n Set Difference\n*****************\n")
    test1 = [('R', []), ('S', [1, 2])]
    test2 = [('R', [1, 3, 5]), ('S', [2, 4])]
    test3 = [('R', range(1, 50001)), ('S', range(2, 50000))]
    mrObject = SetDifferenceMR(test1, 2, 2)
    mrObject.runSystem()
    mrObject = SetDifferenceMR(test2, 2, 2)
    mrObject.runSystem()
    '''
    mrObject = SetDifferenceMR(test3, 2, 2, use_combiner=True)
    mrObject.runSystem()
    '''

    ###################
    ##run Matrix Multiply:
    '''	
    print("\n\n*****************\n Matrix Multiply\n*****************\n")
    #format: 'A|B:A.size:B.size
    test1 = [(('A:2,1:1,2', 0, 0), 2.0), (('A:2,1:1,2', 0, 1), 1.0), (('B:2,1:1,2', 0, 0), 1), (('B:2,1:1,2', 1, 0), 3)   ]
    test2 = createSparseMatrix([[1, 2, 4], [4, 8, 16]], 'A:2,3:3,3') + createSparseMatrix([[1, 1, 1], [2, 2, 2], [4, 4, 4]], 'B:2,3:3,3')

    test3 = createSparseMatrix(np.random.randint(-10, 10, (10,100)), 'A:10,100:100,12') + \
	    createSparseMatrix(np.random.randint(-10, 10, (100,12)), 'B:10,100:100,12')

    #print(test2[:10])
    #print(test3[:10])

    mrObject = MatrixMultMR(test1, 4, 3)
    mrObject.runSystem()

    mrObject = MatrixMultMR(test2, 6, 4)
    mrObject.runSystem()

    mrObject = MatrixMultMR(test3, 16, 10)
    mrObject.runSystem()
    '''