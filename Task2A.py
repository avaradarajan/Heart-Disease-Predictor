from pyspark import SparkContext
from scipy import sparse
import numpy as np


def createSparseMatrix(X, label):
    sparseX = sparse.coo_matrix(X)
    list = []
    for i, j, v in zip(sparseX.row, sparseX.col, sparseX.data):
        list.append(((label, i, j), v))
    return list

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


sc = SparkContext(master="local",appName="spmm")
rdd = sc.parallelize(createSparseMatrix(np.random.randint(-10, 10, (10,100)), 'A:10,100:100,12') + \
	    createSparseMatrix(np.random.randint(-10, 10, (100,12)), 'B:10,100:100,12')).\
    flatMap(lambda k : sparkMap(k[0],k[1])).\
    groupByKey().map(lambda x: (x[0],list(x[1]))).\
    map(lambda k : sparkReduce(k[0], k[1]))
rdd.foreach(lambda k : print(k))


