import re
import hashlib
from operator import add
nonFluencyDictionary = {'mm':'MM','oh':'OH','ah':'OH','si':'SIGH', 'ug':'SIGH', 'uh':'SIGH','um':'UM', 'hm':'UM', 'hu':'UM'}
distinctPhraseCounts = {'MM': 0, 'OH': 0, 'SIGH': 0, 'UM': 0}
#^.*\b((?i)ugh+|(?i)mmm+|(?i)ohh+|(?i)ahh+|(?i)umm*|(?i)hmm*|(?i)huh|(?i)ugh|(?i)uh|(?i)sigh+)\b(.)*\b.*$
def checkMatch(line):
    group = (re.search(
        r'^.*\b(ugh+|mmm+|ohh+|ahh+|umm*|hmm*|huh|ugh|uh|sigh+)\b(.)*\b.*$',str(line[1]),0|re.I))
    if (group):
        return [line[0], line[1]];  # [line[0],line[1].split(group.group(1))[1]]

def checkE(loc):
    print(f'Location -> {loc[0]}---{loc[1]}--{loc[1][0]}')

def returnMatch(line):
    group = re.search(r'^.*\b(ugh+|mmm+|ohh+|ahh+|umm*|hmm*|huh|ugh|uh|sigh+)\b(.)*\b.*$',str(line),0|re.I)
    if (group):
        return [group.group(1),line.split(group.group(1))[1].strip()];

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

def countDist(d):
    gl = 0
    print(f'Location -> {d[0]}- Actual Post -{d[1]}')
    list = (returnMatch(d[1]))
    print(list)
    wordToBeHashed = ""
    if (list is not None):
        print("Still Inside")
        words = list[1].replace(".", "").replace("!", "").split(" ")
        for word in words[:3]:
            if (word):
                wordToBeHashed += word;
        h1 = hashMD5(wordToBeHashed)
        h2 = hashSHA256(wordToBeHashed)
        h3 = hashSHA512(wordToBeHashed)
        print("AFTER HASH")
        if (not (h1 and h2 and h3)):
            if (nonFluencyDictionary.get(list[0][:2].lower())):
                print(distinctPhraseCounts[nonFluencyDictionary[list[0][:2].lower()]])
                distinctPhraseCounts[nonFluencyDictionary[list[0][:2].lower()]] = distinctPhraseCounts.get(nonFluencyDictionary[list[0][:2].lower()]) + 1;
        print(f'{nonFluencyDictionary.get(list[0][:2].lower())} <--> {distinctPhraseCounts[nonFluencyDictionary[list[0][:2].lower()]]}')
        return (nonFluencyDictionary.get(list[0][:2].lower()),distinctPhraseCounts[nonFluencyDictionary[list[0][:2].lower()]]);
    else:
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
    #pprint(nonFluenciesRDD.collect())

    validPostsToCountRDD = nonFluenciesRDD.join(locationRDD).map(lambda x : [x[0],x[1][0]])
    # [('Stony Brook, NY', ('Ugh lost my keys last night', '')), ('Springfield, IL', ('New AWS tool, Data Bricks Ummmmmm why?', ''))]

    # Step 3 - Count distinct elements for a group
    pprint(validPostsToCountRDD.collect())


    #validPostsToCountRDD.foreach(countDist)
    #fl = validPostsToCountRDD.map(lambda x: countDist(x))
    fl = validPostsToCountRDD.map(lambda  x: countDist(x)).filter(lambda x : x is not None).reduceByKey(add)
    pprint(fl.collect())
    sighRDD = fl.filter(lambda x : x[0]=='SIGH').map(lambda x : x[1])
    umRDD = fl.filter(lambda x: x[0] == 'UM').map(lambda x: x[1])
    mmRDD = fl.filter(lambda x: x[0] == 'MM').map(lambda x: x[1])
    ohRDD = fl.filter(lambda x: x[0] == 'OH').map(lambda x: x[1])

    if(not sighRDD.isEmpty()):
        distinctPhraseCounts['SIGH'] = sighRDD.reduce(add)

    if (not umRDD.isEmpty()):
        distinctPhraseCounts['UM'] = umRDD.reduce(add)

    if (not mmRDD.isEmpty()):
        distinctPhraseCounts['MM'] = mmRDD.reduce(add)

    if (not ohRDD.isEmpty()):
        distinctPhraseCounts['OH'] = ohRDD.reduce(add)

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
    '''ds = sc.parallelize([("Stony Brook, NY", "Ugh. lost my keys last night"), ("Somewhere someplace", "Ugh. lost my keys last night"),("Stony Brook, NY", "lost my keys last night"),("Springfield, IL", "New AWS tool, Data Bricks. Ummmmmm why?"),\
                         ("The Big Orange, CA", "Ugh. lost my keys last night"),("Stony Brook, NY", "DUGhh lost my keys last night"),\
                         ("Stony Brook, NYGI", "Ugh. lost my keys last night"),("H-Town, TX", "lost my keys last night")])
    pprint(umbler(sc, ds))'''
    # Umbler Tests:
    print("\n*************************\n Umbler Tests\n*************************")
    testFileSmall = 'C://Users//anand\Documents//PythonProjects//BD1//publicSampleLocationTweet_small.csv'
    testFileLarge = 'C://Users//anand\Documents//PythonProjects//BD1//publicSampleLocationTweet_large.csv'

    # setup rdd
    import csv
    #smallTestRdd = sc.textFile(testFileSmall).mapPartitions(lambda line: csv.reader(line))
    ##pprint(smallTestRdd.take(5))  #uncomment to see data
    #pprint(umbler(sc, smallTestRdd))

    largeTestRdd = sc.textFile(testFileLarge).mapPartitions(lambda line: csv.reader(line))
    ##pprint(largeTestRdd.take(5))  #uncomment to see data
    pprint(umbler(sc, largeTestRdd))

    return

sc = SparkContext(master="local[*]",appName="PythonStreamingNetworkWordCount")
runTests(sc)


'''

    if(fl.filter(lambda x : x[0]=='SIGH').map(lambda x : x[1]).isEmpty()):
        distinctPhraseCounts['SIGH'] = 0;
    else:
        distinctPhraseCounts['SIGH'] = fl.filter(lambda x : x[0]=='SIGH').map(lambda x : x[1]).reduce(add)
    if (fl.filter(lambda x: x[0] == 'UM').map(lambda x: x[1]).isEmpty()):
        distinctPhraseCounts['UM'] = 0;
    else:
        distinctPhraseCounts['UM'] = fl.filter(lambda x: x[0] == 'UM').map(lambda x: x[1]).reduce(add)
    if (fl.filter(lambda x: x[0] == 'MM').map(lambda x: x[1]).isEmpty()):
        distinctPhraseCounts['MM'] = 0;
    else:
        distinctPhraseCounts['MM'] = fl.filter(lambda x: x[0] == 'MM').map(lambda x: x[1]).reduce(add)
    if (fl.filter(lambda x: x[0] == 'OH').map(lambda x: x[1]).isEmpty()):
        distinctPhraseCounts['OH'] = 0;
    else:
        distinctPhraseCounts['OH'] = fl.filter(lambda x: x[0] == 'OH').map(lambda x: x[1]).reduce(add)
'''