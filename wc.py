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
                    #print(f'here{namenode_m2r}')

    def partitionFunction(self, k): #A based on the key characters it allocates a node for processing
        # given a key returns the reduce task to send it
        node_number = np.sum([ord(c) for c in str(k)]) % self.num_reduce_tasks
        #print(f'here{node_number}')
        return node_number

    def reduceTask(self, kvs, namenode_fromR):
        # sort all values for each key (can use a list of dictionary)
        vsPerK = dict()
        for (k, v) in kvs:
            try:
                vsPerK[k].append(v)
            except KeyError:
                vsPerK[k] = [v]
        #pprint(vsPerK)
        # call reducers on each key with a list of values
        # and append the result for each key to namenoe_fromR
        for k, vs in vsPerK.items():
            if vs:
                fromR = self.reduce(k, vs) #gives back (k,sum(count))
                if fromR:  # skip if reducer returns nothing (no data to pass along)
                    namenode_fromR.append(fromR) #A output from reducer
        pprint(namenode_fromR)

    def runSystem(self):
        # runs the full map-reduce system processes on mrObject

        # [SEGMENT 1]
        # What: The runSystem is like a master that handles the various task involved in map and reduce. In this segment we are creating two lists.
        #namenode_m2r is to hold the output from Map to eventually contain (reducer it should go to, list of kv pairs for that reducer)
        #namenode_fromR is to hold the output from the reducer step to eventually contain the final result of say word count.
        # Why: <your reasoning here>
        #
        # the following two lists are shared by all processes
        # in order to simulate the communication
        namenode_m2r = Manager().list()  # stores the reducer task assignment and
        # each key-value pair returned from mappers
        # in the form: [(reduce_task_num, (k, v)), ...]
        # [COMBINER: when enabled this might hold]
        namenode_fromR = Manager().list()  # stores key-value pairs returned from reducers
        # in the form [(k, v), ...]

        # [SEGMENT 2]
        # What: <your description here>
        #
        # Why: <your reasoning here>
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
            processes[-1].start() #A start the last process added to the list
            chunkStart = chunkEnd

        # [SEGMENT 3]
        # What: <your description here>
        #
        # Why: <your reasoning here>
        #
        # join map task processes back
        for p in processes:
            p.join()
            # print output from map tasks
        print("namenode_m2r after map tasks complete:")
        #pprint(sorted(list(namenode_m2r)))#A basically pretty print breaks the output to single line to make it easier for interpreter

        ##[SEGMENT 4]
        # What: <your description here>
        #
        # Why: <your reasoning here>
        #
        # "send" each key-value pair to its assigned reducer by placing each
        # into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
        to_reduce_task = [[] for i in range(self.num_reduce_tasks)] #A create empty lists inside outer list based on num of reduce task
        for (rtask_num, kv) in namenode_m2r:
            to_reduce_task[rtask_num].append(kv)#A assign all the key value pairs to a specific reducer core as a list of kv pairs in that index
        #pprint(to_reduce_task)
        # [SEGMENT 5]
        # What: <your description here>
        #
        # Why: <your reasoning here>
        #
        # launch the reduce tasks as a new process for each.
        processes = []
        for kvs in to_reduce_task:#A take each inner list item containing list of kv pairs assigned to that reducer and reduce
            processes.append(Process(target=self.reduceTask, args=(kvs, namenode_fromR)))
            processes[-1].start()

        # [SEGMENT 6]
        # What: <your description here>
        #
        # Why: <your reasoning here>
        #
        # join the reduce tasks back
        for p in processes:
            p.join()
        # print output from reducer tasks
        print("namenode_fromR after reduce tasks complete:")
        pprint(sorted(list(namenode_fromR)))

        # return all key-value pairs:
        return namenode_fromR



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

if __name__ == "__main__":  # [Uncomment peices to test]

    ###################
    ##run WordCount:

    print("\n\n*****************\n Word Count\n*****************\n")
    data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "Car engines purred and the tires burned.")]
    print("\nWord Count Basic WITHOUT Combiner:")
    mrObjectNoCombiner = WordCountBasicMR(data, 2, 3)
    mrObjectNoCombiner.runSystem()
    '''
    print("\nWord Count Basic WITH Combiner:")
    mrObjectWCombiner = WordCountBasicMR(data, 4, 3, use_combiner=True)
    mrObjectWCombiner.runSystem()
    '''
