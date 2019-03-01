# Anandh Varadarajan, 112505082

# Template code for CSE545 - Spring 2019
# Assignment 1 - Part II
# v0.01
from pyspark import SparkContext
import re
import hashlib
from operator import add
import psutil #removes WARNS from console
nonFluencyDictionary = {'mm':'MM','oh':'OH','ah':'OH','si':'SIGH', 'ug':'SIGH', 'uh':'SIGH','um':'UM', 'hm':'UM', 'hu':'UM'}

'''
This function returns only records that has the non-fluencies
'''
def checkMatch(line):
    group = (re.search(
        r'^.*\b(mm+|oh+|ah+|umm*|hmm*|huh|ugh|uh|sigh+)\b(.)*\b.*$',str(line[1]),0|re.I))
    if (group):
        return [line[0], line[1]];

'''
This function returns a tuple of the form (non-fluency group, text following group) for further reduction
'''
def returnMatch(line):
    group = re.search(r'^.*\b(mm+|oh+|ah+|umm*|hmm*|huh|ugh|uh|sigh+)\b(.)*\b.*$',str(line),0|re.I)
    if (group):
        return [group.group(1),re.sub(r"[\\?+ | \\:+ | \\*+ | \\#+ | \\@+ | \\;+ | \\,+ | \\)+ | \\(+]"," ",line.split(group.group(1))[1]).strip()];

#Creating an array for the Bloom Filter
binArray = [0 for i in range(10000)]

def hashMD5(word):
    hash1 = False;
    sum = 0;
    hashValue = 0;
    hv = hashlib.md5(word.encode())
    for i in hv.hexdigest():
        sum = sum + ord(i)
    hashValue = sum%10000;
    if(binArray[hashValue]==1):
        hash1 = True;
    else:
        hash1 = False;
        binArray[hashValue] = 1;
    return hash1;

def hashSHA256(word):
    hash2 = False;
    sum = 0;
    hashValue = 0;
    hv = hashlib.sha3_256(word.encode())
    for i in hv.hexdigest():
        sum = sum + ord(i)
    hashValue = sum % 10000;
    if (binArray[hashValue] == 1):
        hash2 = True;
    else:
        hash2 = False;
        binArray[hashValue] = 1;
    return hash2;
def hashSHA512(word):
    hash3 = False;
    sum = 0;
    hashValue = 0;
    hv = hashlib.sha3_256(word.encode())
    for i in hv.hexdigest():
        sum = sum + ord(i)
    hashValue = sum % 10000;
    if (binArray[hashValue] == 1):
        hash3 = True;
    else:
        hash3 = False;
        binArray[hashValue] = 1;
    return hash3;

def sparkMap(k, v):
    kvs = []
    row = int(k[1])  # current row number of value
    col = int(k[2])  # current column number of value
    matrix = k[0][:k[0].find(":")]  # Matrix label

    # Following are the parsers to extract values like nA,nB,mA,mB from the input data
    parserOne = k[0].find(":")
    parserTwo = k[0].find(",")
    parserThree = k[0].find(":", parserOne + 1)
    parserFour = k[0].find(",", parserTwo + 1)

    i = int(k[0][parserOne + 1:parserTwo])  # row count of Matrix A
    j = int(k[0][parserTwo + 1:parserThree])  # row count of Matrix B
    kval = int(k[0][parserFour + 1:])  # column count of Matrix B

    if ('A' in matrix):
        for loop in range(kval):
            kvs.append(((row + 1, loop + 1), (matrix + str(i) + "," + str(j), col + 1, v)))
        return kvs;
    else:
        for loop in range(i):
            kvs.append(((loop + 1, col + 1), (matrix + str(j) + "," + str(kval), row + 1, v)))
        return kvs;


def sparkReduce(k, vs):
    def sortByJ(val):
        return val[1];

    A = []
    B = []

    # Create lists with each matrix
    for v in vs:
        if ('A' in v[0]):
            # resultantMatrix_rowValue = v[0][1:valueParser];
            A.append(v)
        else:
            # resultantMatrix_colValue = v[0][valueParser + 1:];
            B.append(v)

    A.sort(key=sortByJ)
    B.sort(key=sortByJ)

    # Extract the row and column count of the resultant matrix
    resultantmatrix_rowvalue = 0
    resultantmatrix_colvalue = 0
    valueParser = A[0][0].find(",")
    resultantmatrix_rowvalue = A[0][0][1:valueParser];
    valueParser = B[0][0].find(",")
    resultantmatrix_colvalue = B[0][0][valueParser + 1:];

    sum = 0;
    for a, b in zip(A, B):
        sum = sum + a[2] * b[2];
    return (('AXB:' + str(resultantmatrix_rowvalue) + "," + str(resultantmatrix_colvalue), k[0] - 1, k[1] - 1), sum)

#loads the count values into corresponding dictionary. 
def loadResultDictionary(filteredAndCountedRdd,distinctPhraseCounts):
    sighRDD = filteredAndCountedRdd.filter(lambda x: x[0] == 'SIGH').map(lambda x: x[1])
    umRDD = filteredAndCountedRdd.filter(lambda x: x[0] == 'UM').map(lambda x: x[1])
    mmRDD = filteredAndCountedRdd.filter(lambda x: x[0] == 'MM').map(lambda x: x[1])
    ohRDD = filteredAndCountedRdd.filter(lambda x: x[0] == 'OH').map(lambda x: x[1])

    if (not sighRDD.isEmpty()):
        distinctPhraseCounts['SIGH'] = sighRDD.reduce(add)

    if (not umRDD.isEmpty()):
        distinctPhraseCounts['UM'] = umRDD.reduce(add)

    if (not mmRDD.isEmpty()):
        distinctPhraseCounts['MM'] = mmRDD.reduce(add)

    if (not ohRDD.isEmpty()):
        distinctPhraseCounts['OH'] = ohRDD.reduce(add)


def sparkMatrixMultiply(rdd):
    # rdd where records are of the form:
    #  ((â€œA:nA,mA:nB,mBâ€, row, col),value)
    # returns an rdd with the resulting matrix

    resultRdd = rdd.flatMap(lambda k : sparkMap(k[0],k[1])).\
    groupByKey().map(lambda x: (x[0],list(x[1]))).\
    map(lambda k : sparkReduce(k[0], k[1])).sortByKey(ascending=True)
    return resultRdd

def umbler(sc, rdd):
    # sc: the current spark context
    #    (useful for creating broadcast or accumulator variables)
    # rdd: an RDD which contains location, post data.
    #
    # returns a *dictionary* (not an rdd) of distinct phrases per um category
    distinctPhraseCounts = {'MM': 0, 'OH': 0, 'SIGH': 0, 'UM': 0}

    #Load the locations in an RDD
    locationRDD = sc.textFile('C://Users//anand//Downloads//convertcsv.csv').map(lambda inp: inp.replace('"', "")).map(
        lambda x: (x, ''))

    #Load the RDD with records containing only the non-fluencies
    nonFluenciesRDD = rdd.map(lambda x: list(x)).map(lambda x: checkMatch(x)).filter(lambda x: x != None).map(
        lambda x: (x[0], str(x[1]).replace(".", "").strip()))

    #Join the NF RDD with location RDD -> To get another RDD that includes final records to be processed for distinct words following non-fluency
    validPostsToCountRDD = nonFluenciesRDD.join(locationRDD).map(lambda x : [x[0],x[1][0]])

    # SETUP for streaming algorithms
    #custom bloom filter implementation for distinct word count
    def countDistinctUsingStreamingAlgorithm(d):
        list = (returnMatch(d[1]))
        wordToBeHashed = ""
        if (list is not None):
            words = list[1].replace(".", "").replace("!", "").split(" ")
            for word in words[:3]:
                if (word):
                    wordToBeHashed += word;
            h1 = hashMD5(wordToBeHashed)
            h2 = hashSHA256(wordToBeHashed)
            h3 = hashSHA512(wordToBeHashed)
            if (not (h1 and h2 and h3)):
                if (nonFluencyDictionary.get(list[0][:2].lower())):
                    distinctPhraseCounts[nonFluencyDictionary[list[0][:2].lower()]] = distinctPhraseCounts.get(
                        nonFluencyDictionary[list[0][:2].lower()]) + 1;
                    return (nonFluencyDictionary.get(list[0][:2].lower()),
                    1);
                else:
                    return (nonFluencyDictionary.get(list[0][:2].lower()),
                        0);


    filteredAndCountedRdd = validPostsToCountRDD.map(lambda  x: countDistinctUsingStreamingAlgorithm(x)).filter(lambda x : x is not None).reduceByKey(add)

    loadResultDictionary(filteredAndCountedRdd,distinctPhraseCounts)

    return distinctPhraseCounts


################################################
## Testing Code (subject to change for testing)

import numpy as np
from pprint import pprint
from scipy import sparse


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
    test3 = createSparseMatrix(np.random.randint(-10, 10, (10, 100)), 'A:10,100:100,12') + createSparseMatrix(
        np.random.randint(-10, 10, (100, 12)), 'B:10,100:100,12')
    mmResults = sparkMatrixMultiply(sc.parallelize(test1))
    pprint(mmResults.collect())
    mmResults = sparkMatrixMultiply(sc.parallelize(test2))
    pprint(mmResults.collect())
    mmResults = sparkMatrixMultiply(sc.parallelize(test3))
    pprint(mmResults.collect())

    #Umbler Tests:
    print("\n*************************\n Umbler Tests\n*************************")
    testFileSmall = 'C://Users//anand\Documents//PythonProjects//BD1//publicSampleLocationTweet_small.csv'
    testFileLarge = 'C://Users//anand\Documents//PythonProjects//BD1//publicSampleLocationTweet_large.csv'

    # setup rdd
    import csv

    smallTestRdd = sc.textFile(testFileSmall).mapPartitions(lambda line: csv.reader(line))
    #pprint(smallTestRdd.take(5))  #uncomment to see data
    pprint(umbler(sc, smallTestRdd))

    largeTestRdd = sc.textFile(testFileLarge).mapPartitions(lambda line: csv.reader(line))
    ##pprint(largeTestRdd.take(5))  #uncomment to see data
    pprint(umbler(sc, largeTestRdd))

    return

sc = SparkContext(master="local[*]",appName="PythonStreamingNetworkWordCount")
runTests(sc)
sc.stop()
