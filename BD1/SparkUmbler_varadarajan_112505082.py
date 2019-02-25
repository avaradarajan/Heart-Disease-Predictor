# name, id

# Template code for CSE545 - Spring 2019
# Assignment 1 - Part II
# v0.01

def sparkMap(k,v):
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
    if(matrix=="A"):
        for loop in range(kval):
            kvs.append(((row+1,loop+1),(matrix,col+1,v)))
        return kvs;
    else:
        for loop in range(i):
            kvs.append(((loop+1, col+1), (matrix, row+1, v)))
        return kvs;
def sparkReduce(k, vs):
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
    return (('AXB', k[0] - 1, k[1] - 1), sum)


def sparkMatrixMultiply(rdd):
    # rdd where records are of the form:
    #  ((â€œA:nA,mA:nB,mBâ€, row, col),value)
    # returns an rdd with the resulting matrix

    resultRdd = rdd.flatMap(lambda k : sparkMap(k[0],k[1])).\
    groupByKey().map(lambda x: (x[0],list(x[1]))).\
    map(lambda k : sparkReduce(k[0], k[1]))
    return resultRdd


def umbler(sc, rdd):
    # sc: the current spark context
    #    (useful for creating broadcast or accumulator variables)
    # rdd: an RDD which contains location, post data.
    #
    # returns a *dictionary* (not an rdd) of distinct phrases per um category

    # SETUP for streaming algorithms

    # PROCESS the records in RDD (i.e. as if processing a stream:
    # a foreach transformation
    filteredAndCountedRdd = rdd  # REVISE TO COMPLETE
    #  (will probably require multiple lines and/or methods)

    # return value should fit this format:
    distinctPhraseCounts = {'MM': 0,
                            'OH': 0,
                            'SIGH': 0,
                            'UM': 0}

    return distinctPhraseCounts


################################################
## Testing Code (subject to change for testing)

import numpy as np
from pprint import pprint
from scipy import sparse
from pyspark import SparkContext

def createSparseMatrix(X, label):
    sparseX = sparse.coo_matrix(X)
    list = []
    for i, j, v in zip(sparseX.row, sparseX.col, sparseX.data):
        list.append(((label, i, j), v))
    return list


def runTests(sc):
    # runs MM and Umbler Tests for the given sparkContext

    # MM Tests:
    print("\n*************************\n MatrixMult Tests\n*************************")
    test1 = [(('A:1,2:2,1', 0, 0), 2.0), (('A:1,2:2,1', 0, 1), 1.0), (('B:1,2:2,1', 0, 0), 1), (('B:1,2:2,1', 1, 0), 3)]
    test2 = createSparseMatrix([[1, 2, 4], [4, 8, 16]], 'A:2,3:3,3') + createSparseMatrix(
        [[1, 1, 1], [2, 2, 2], [4, 4, 4]], 'B:2,3:3,3')
    #test3 = createSparseMatrix(np.random.randint(-10, 10, (10, 100)), 'A:10,100:100,12') + createSparseMatrix(
    #    np.random.randint(-10, 10, (100, 12)), 'B:10,100:100,12')
    mmResults = sparkMatrixMultiply(sc.parallelize(test1))
    pprint(mmResults.collect())
    mmResults = sparkMatrixMultiply(sc.parallelize(test2))
    pprint(mmResults.collect())
    #mmResults = sparkMatrixMultiply(sc.parallelize(test3))
    #pprint(mmResults.collect())

    '''
    # Umbler Tests:
    print("\n*************************\n Umbler Tests\n*************************")
    testFileSmall = 'publicSampleLocationTweet_small.csv'
    testFileLarge = 'publicSampleLocationTweet_large.csv'

    # setup rdd
    import csv
    smallTestRdd = sc.textFile(testFileSmall).mapPartitions(lambda line: csv.reader(line))
    ##pprint(smallTestRdd.take(5))  #uncomment to see data
    pprint(umbler(sc, smallTestRdd))

    largeTestRdd = sc.textFile(testFileLarge).mapPartitions(lambda line: csv.reader(line))
    ##pprint(largeTestRdd.take(5))  #uncomment to see data
    pprint(umbler(sc, largeTestRdd))
'''
    return

sc = SparkContext(appName="PythonStreamingNetworkWordCount")
runTests(sc)