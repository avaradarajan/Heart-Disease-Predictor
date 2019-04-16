# name, id

# Template code for CSE545 - Spring 2019
# Assignment 1 - Part II
# v0.01
from collections import Counter

from pyspark import SparkConf, SparkContext
import hashlib
import re
import numpy as np
import itertools

"""
    BLOOM Filter
    Reference#1: https://en.wikipedia.org/wiki/Bloom_filter
    Reference#2: https://llimllib.github.io/bloomfilter-tutorial/ 
"""


class BloomFilterUtil:
    """ Implementation of Bloom Filter """
    """
        Mapping 
        detection_groups = [('(\\bmm+\\b)', "mm+", "MM"),
                            ('(\\boh+\\b)', "oh+", "OH"),
                            ('(\\bah+\\b)', "ah+", "OH"),
                            ('(\\bsigh(?:|ing|s|ed)\\b)', "sigh+", "SIGH"),
                            ('(\\bugh\\b)', "sigh+", "SIGH"),
                            ('(\\buh\\b)', "sigh+", "SIGH"),
                            ('(\\bumm*\\b)', "umm*", "UM"),
                            ('(\\bhmm*\\b)', "hmm*", "UM"),
                            ('(\\bhuh\\b)', "huh", "UM")]
        
    """
    def __init__(self, m, k, hash_fun):
        self.data = dict()
        self.false_positive = 0
        # Vector Size
        self.vec_size = m
        # Vector of vec_size
        self.vector = [0] * m
        # k <-- number of hashes to be computed
        self.k = k
        self.hash_fun = hash_fun

    def add(self, key, value=True):
        # save the key with default value True
        # can be inherited to use more advanced version
        self.data[key] = value
        for i in range(self.k):
            self.vector[self.hash_fun(key + str(i)) % self.vec_size] = 1

    def contains(self, key):
        """Check inclusion

        :param key: search in self.vector
        :return:
        """
        for i in range(self.k):
            if self.vector[self.hash_fun(key + str(i)) % self.vec_size] == 0:
                return False
        return True

    def get(self, value_to_search):
        """GET a matched nonfluencies

        :param value_to_search: str : string
        :return: return the list of Mapped nonfluencies as list
        // Create a map of regex vs simplified expression vs nonfluencies
        // find the match with regex mather and reduce the query words to reduced format
        // search only reduced formats in the keylist i.e with bloom filters

        # verified regex on pythex : '(mm*)|(oh*)|(ah*)|(sigh(?:|ing|s|ed))|(ugh)|(uh)|(umm*)|(hmm*)|(huh)'
        """
        # apply regex
        detection_groups = [('(\\bmm+\\b)', "mm+", "MM"),
                            ('(\\boh+\\b)', "oh+", "OH"),
                            ('(\\bah+\\b)', "ah+", "OH"),
                            ('(\\bsigh(?:|ing|s|ed)\\b)', "sigh+", "SIGH"),
                            ('(\\bugh\\b)', "sigh+", "SIGH"),
                            ('(\\buh\\b)', "sigh+", "SIGH"),
                            ('(\\bumm*\\b)', "umm*", "UM"),
                            ('(\\bhmm*\\b)', "hmm*", "UM"),
                            ('(\\bhuh\\b)', "huh", "UM")]
        all_groups = [i for i in re.findall("|".join(list(map(lambda x: x[0], detection_groups))), value_to_search)]
        all_groups_with_indexes = [[{index: j} for index, j in enumerate(i)] for i in all_groups]
        concat = list(map(lambda x: (list(x.items())[0]), np.array(all_groups_with_indexes).flatten().tolist()))
        concat_filtered = list(filter(lambda x: x[1] != "", concat))
        cnt = list(map(lambda x: (x[1], detection_groups[x[0]][1], detection_groups[x[0]][2]), concat_filtered))
        return_vals = []
        for WORD, WORD2, red_repr in cnt:
            if self.contains(WORD2):
                try:
                    ret = self.data[WORD2]  # actual lookup
                    if ret:
                        return_vals.append(red_repr)
                except KeyError:
                    self.false_positive += 1
        return list(set(return_vals))


def hash_function(x):
    """SHA256 based hash function

    :param x: input string
    :return: DIGEST KEY
    """
    h = hashlib.sha1((x).encode('utf-8'))
    return int(h.hexdigest(), base=16)

def smap(k, v):
        # <COMPLETE>
        p1 = str(k[0]).find(':')
        p2 = str(k[0]).find(',')
        p3 = str(k[0]).find(':',p2+1)
        p4 = str(k[0]).find(',',p2+1)
        p5 = str(k[1])
        p6 = str(k[2])
        # print("p5 is", p5)
        # print("p6 is", p6)
        # print("p4 is", p4)
        ret = {}
        list1, list2 = [],[]
        # print(f'{k[0][p1+1:p2]}')
        # print(f'{k[0][p2 + 1:p3]}')
        # print(f'{k[0][p4+1:]}')
        g1 = k[0][p1+1:p2] # i --> row number of first matrix
        g2 = k[0][p2 + 1:p3]# j --> column of first matrix
        g3 = k[0][p4+1:]# k --> column number of second matrix4

        # return ({k[0][p1 + 1:p2]}, {k[0][p2 + 1:p3]}, {k[0][p2 + 1:p3]}, (v, v))
        if k[0][0] == "A":
            for i in range(0, int(g3)):
               p7 = "A"
               # list1.append(((int(g1),i),(p7,int(g2),v)))
               list1.append(((int(p5)+1, i+1), (p7, int(p6)+1, v)))
            return list1
        else:
            for i in range(0, int(g1)):
                p10 = "B"
                list2.append(((i+1,int(p6)+1),(p10,int(p5)+1,v)))
            return list2


def sreduce(k, vs):
        # <COMPLETE>
        list3, list4 = [], []
        for v in vs:
            if v[0][0] == "A":
                list3.append(v)
            else:
                list4.append(v)
        result = 0
        i = 0
        list3.sort(key = lambda v : v[1])
        list4.sort(key=lambda v: v[1])

        sum = 0
        pt = []
        for i,j in zip(list3,list4):
           pt = i[2] * j[2]
           sum += pt

        return (('AXB',k[0],k[1]),sum)


def sparkMatrixMultiply(rdd):
    # rdd where records are of the form:
    #  ((â€œA:nA,mA:nB,mBâ€, row, col),value)
    # returns an rdd with the resulting matrix
    resultRdd = rdd.flatMap(lambda x : smap(x[0],x[1])).\
    groupByKey().map(lambda y : (y[0],list(y[1]))).\
    map(lambda z : sreduce(z[0],z[1]))
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
    # create a bloom filter
    bm = BloomFilterUtil(10000, 10, hash_function)
    for word in ["mm+", "oh+", "ah+", "sigh+", "ugh", "uh", "umm*", "hmm*", "huh"]:
        bm.add(word)

    locations = []
    # reading location.csv from data folder
    # assuming exact match for location as written in assignment
    # list of valid locations is given the file: umbler_locations.csv
    with open("umblerData/umbler_locations.csv") as fs:
        locations = [i.rstrip() for i in fs.readlines()]

    # RDD-> LOC, TWEET, Check if Location is Valid, Check if words in Tweet matches with non-fluencies using BloomFilter
    smallTestRdd = rdd.map(lambda x: (x[0], x[1], x[0].strip() in locations, bm.get(x[1])))
    # RDD->take only matched list of  non-fluencies
    all_matched_keys = smallTestRdd.map(lambda x: x[-1]).collect()
    # filter and count number of occurrences of non-fluencies in all tweets
    all_matched_keys = Counter(
        list(itertools.chain(*[i for i in list(filter(lambda x: len(x) != 0, all_matched_keys))])))

    distinctPhraseCounts = {'MM': 0,
                            'OH': 0,
                            'SIGH': 0,
                            'UM': 0}
    # copy in desired formats
    for k, _ in distinctPhraseCounts.items():
        if k in all_matched_keys.keys():
            V = all_matched_keys[k]
            distinctPhraseCounts[k] = V

    # print(distinctPhraseCounts)
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

    # Umbler Tests:
    print("\n*************************\n Umbler Tests\n*************************")
    testFileSmall = 'umblerData\publicSampleLocationTweet_small.csv'
    testFileLarge = 'umblerData\publicSampleLocationTweet_large.csv'

    # setup rdd
    import csv
    print("-- SMALL --")
    smallTestRdd = sc.textFile(testFileSmall).mapPartitions(lambda line: csv.reader(line))
    #pprint(smallTestRdd.take(5))  # uncomment to see data
    pprint(umbler(sc, smallTestRdd))

    print("-- LARGE --")
    largeTestRdd = sc.textFile(testFileLarge).mapPartitions(lambda line: csv.reader(line))
    ##pprint(largeTestRdd.take(5))  #uncomment to see data
    pprint(umbler(sc, largeTestRdd))

    return


if __name__ == '__main__':
    # def get_spark_context():
    #     conf = SparkConf().setAppName('demo-app-name').set('spark.executor.memory', '3g').set('spark.driver.memory',
    #                                                                                           '2g')
    #     sc = SparkContext(master='local[2]', conf=conf).getOrCreate()
    #     return sc

    conf = SparkConf().setAppName("app")
    sc = SparkContext(conf=conf)

    runTests(sc)
