from pyspark import SparkContext
from scipy import sparse

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
rdd = sc.parallelize(createSparseMatrix([[1, 2, 4], [4, 8, 16]], 'A:2,3:3,3') + createSparseMatrix([[1, 1, 1], [2, 2, 2], [4, 4, 4]], 'B:2,3:3,3'))
#print(rdd.take(2))
#[(('A:2,3:3,3', 0, 0), 1), (('A:2,3:3,3', 0, 1), 2)]
mrdd = rdd.flatMap(lambda k : sparkMap(k[0],k[1]))
#print(mrdd.collect())
#[((1, 1), ('A', 1, 1)), ((1, 2), ('A', 1, 1)), ((1, 3), ('A', 1, 1)), ((1, 1), ('A', 2, 2)), ((1, 2), ('A', 2, 2)), ((1, 3), ('A', 2, 2)), ((1, 1), ('A', 3, 4)), ((1, 2), ('A', 3, 4)), ((1, 3), ('A', 3, 4)), ((2, 1), ('A', 1, 4)), ((2, 2), ('A', 1, 4)), ((2, 3), ('A', 1, 4)), ((2, 1), ('A', 2, 8)), ((2, 2), ('A', 2, 8)), ((2, 3), ('A', 2, 8)), ((2, 1), ('A', 3, 16)), ((2, 2), ('A', 3, 16)), ((2, 3), ('A', 3, 16)), ((1, 1), ('B', 1, 1)), ((2, 1), ('B', 1, 1)), ((1, 2), ('B', 1, 1)), ((2, 2), ('B', 1, 1)), ((1, 3), ('B', 1, 1)), ((2, 3), ('B', 1, 1)), ((1, 1), ('B', 2, 2)), ((2, 1), ('B', 2, 2)), ((1, 2), ('B', 2, 2)), ((2, 2), ('B', 2, 2)), ((1, 3), ('B', 2, 2)), ((2, 3), ('B', 2, 2)), ((1, 1), ('B', 3, 4)), ((2, 1), ('B', 3, 4)), ((1, 2), ('B', 3, 4)), ((2, 2), ('B', 3, 4)), ((1, 3), ('B', 3, 4)), ((2, 3), ('B', 3, 4))]
res = mrdd.groupByKey().map(lambda x: (x[0],list(x[1])))
#print(res.collect())
#[((1, 1), [('A', 1, 1), ('A', 2, 2), ('A', 3, 4), ('B', 1, 1), ('B', 2, 2), ('B', 3, 4)]), ((1, 2), [('A', 1, 1), ('A', 2, 2), ('A', 3, 4), ('B', 1, 1), ('B', 2, 2), ('B', 3, 4)]), ((1, 3), [('A', 1, 1), ('A', 2, 2), ('A', 3, 4), ('B', 1, 1), ('B', 2, 2), ('B', 3, 4)]), ((2, 1), [('A', 1, 4), ('A', 2, 8), ('A', 3, 16), ('B', 1, 1), ('B', 2, 2), ('B', 3, 4)]), ((2, 2), [('A', 1, 4), ('A', 2, 8), ('A', 3, 16), ('B', 1, 1), ('B', 2, 2), ('B', 3, 4)]), ((2, 3), [('A', 1, 4), ('A', 2, 8), ('A', 3, 16), ('B', 1, 1), ('B', 2, 2), ('B', 3, 4)])]
res.foreach(lambda k : print(type(k[1])))
res2 = res.map(lambda k : sparkReduce(k[0], k[1]))
res2.foreach(lambda k : print(k))


