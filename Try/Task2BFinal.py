import re

nonFluencyDictionary = {'mm':'MM','oh':'OH','ah':'OH','si':'SIGH', 'ug':'SIGH', 'uh':'SIGH','um':'UM', 'hm':'UM', 'hu':'UM'}


def checkMatch(line):
    group = (re.search(
        r'^.*\b((?i)ugh+|(?i)mm+|(?i)oh+|(?i)ah+|(?i)um+|(?i)hm+|(?i)huh+|(?i)ugh+|(?i)uh+|(?i)sigh+)(.)*\b.*$',
        str(line[1])))
    if (group):
        return [line[0], line[1]];  # [line[0],line[1].split(group.group(1))[1]]

def checkE(loc):
    print(f'Location -> {loc[0]}---{loc[1]}--{loc[1][0]}')

def returnMatch(line):
    group = (re.search(
        r'^.*\b((?i)ugh+|(?i)mm+|(?i)oh+|(?i)ah+|(?i)um+|(?i)hm+|(?i)huh+|(?i)ugh+|(?i)uh+|(?i)sigh+)(.)*\b.*$',
        str(line[1])))
    if (group):
        return [line[0], line[1]];  # [line[0],line[1].split(group.group(1))[1]]
def countDist(loc):
    print(f'Location -> {loc[0]}- Actual Post -{loc[1]}')
    return;


def umbler(sc, rdd):
    # sc: the current spark context
    #    (useful for creating broadcast or accumulator variables)
    # rdd: an RDD which contains location, post data.
    #
    locationRDD = sc.textFile('C://Users//anand//Downloads//convertcsv.csv').map(lambda inp: inp.replace('"', "")).map(
        lambda x: (x, ''))

    nonFluenciesRDD = rdd.map(lambda x: list(x)).map(lambda x: checkMatch(x)).filter(lambda x: x != None).map(
        lambda x: (x[0], str(x[1]).replace(".", "").strip()))
    print(nonFluenciesRDD.collect())

    validPostsToCountRDD = nonFluenciesRDD.join(locationRDD).map(lambda x : [x[0],x[1][0]])
    # [('Stony Brook, NY', ('Ugh lost my keys last night', '')), ('Springfield, IL', ('New AWS tool, Data Bricks Ummmmmm why?', ''))]

    # Step 3 - Count distinct elements for a group
    print(validPostsToCountRDD.collect())
    #validPostsToCountRDD.foreach(countDist)
    '''
    def checkE(loc):
        print(f'Location -> {loc[0]}---{loc[1]}--{loc[1][0]}')
    '''
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

def runTests(sc):
    # Umbler Tests:
    print("\n*************************\n Umbler Tests\n*************************")
    ds = sc.parallelize([("Stony Brook, NY", "Ugh. lost my keys last night"), ("Somewhere someplace", "Ugh. lost my keys last night"),("Stony Brook, NY", "lost my keys last night"),("Springfield, IL", "New AWS tool, Data Bricks. Ummmmmm why?"),\
                         ("The Big Orange, CA", "Ugh. lost my keys last night"),("Stony Brook, NY", "DUGhh lost my keys last night"),\
                         ("Stony Brook, NYGI", "Ugh. lost my keys last night"),("H-Town, TX", "lost my keys last night")])
    pprint(umbler(sc, ds))
    return

sc = SparkContext(appName="PythonStreamingNetworkWordCount")
runTests(sc)