# Anandh Varadarajan, 112505082

# Template code for CSE545 - Spring 2019
# Assignment 1 - Part IIB) Extra Credit Code - Works with netcat setup in local
# v1
from pyspark import SparkContext
import re
import hashlib
from operator import add
from pyspark.streaming import StreamingContext

nonFluencyDictionary = {'mm':'MM','oh':'OH','ah':'OH','si':'SIGH', 'ug':'SIGH', 'uh':'SIGH','um':'UM', 'hm':'UM', 'hu':'UM'}

'''
This function returns only records that has the non-fluencies
'''
def checkMatch(line):
    group = (re.search(
        r'^.*\b(mmm+|ohh+|ahh+|umm*|hmm*|huh|ugh|uh|sigh+)\b(.)*\b.*$',str(line[1]),0|re.I))
    if (group):
        return [line[0], line[1]];

'''
This function returns a tuple of the form (non-fluency group, text following group) for further reduction
'''
def returnMatch(line):
    group = re.search(r'^.*\b(mmm+|ohh+|ahh+|umm*|hmm*|huh|ugh|uh|sigh+)\b(.)*\b.*$',str(line),0|re.I)
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

distinctPhraseCounts = {'MM': 0, 'OH': 0, 'SIGH': 0, 'UM': 0}

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
                return (nonFluencyDictionary.get(list[0][:2].lower()),1);
            else:
                return (nonFluencyDictionary.get(list[0][:2].lower()),0);


#Creating spark context
sc = SparkContext(master="local[2]",appName="UmblerStreaming")

#Creating streaming context
ssc = StreamingContext(sc, 2)

#Read locations from csv file
locationRDD = sc.textFile('C://Users//anand//Downloads//convertcsv.csv').map(lambda inp: inp.replace('"', "")).map(
        lambda x: (x, ''))

#Read input from server/stream at port 9999
input = ssc.socketTextStream("localhost",9999)

#Parsing
words = input.flatMap(lambda line: line.split('),('))
stage1 = words.map(lambda line: [line.split(',"')[0],line.split(',"')[1]])
onlyNonFluencyStage = stage1.map(lambda x: checkMatch(x))
stage3 = onlyNonFluencyStage.filter(lambda x: x != None)
stage4 = stage3.map(lambda x: (str(x[0]).replace("(","").replace('"',"").strip(), str(x[1]).replace(".", "").strip()))
locationMatch = stage4.transform(lambda x : x.join(locationRDD))
stage6 = locationMatch.map(lambda x : [x[0],x[1]])
stage7 = stage6.map(lambda  x: [x[0],x[1][0]])
countDS = stage7.map(lambda  x: countDistinctUsingStreamingAlgorithm(x))
clearEmpty = countDS.filter(lambda x : x is not None)
finalRDD = clearEmpty.reduceByKey(add)
distinctPhraseCounts = loadResultDictionary(finalRDD,distinctPhraseCounts)
distinctPhraseCounts.pprint()

#start listening and getting inputs
ssc.start()
ssc.awaitTermination()


#----------------------------
#Input format
'''
("Stony Brook, NY","Ugh. lost my keys last night"),("Womewhere someplace","Ugh. lost my keys last night"),("Stony Brook, NY","lost my keys last night"),("Springfield, IL","New AWS tool, Data Bricks. Ummmmmm why?")
'''

#Intermediate output
'''
['Stony Brook, NY', 'Ugh lost my keys last night"']
['Springfield, IL', 'New AWS tool, Data Bricks Ummmmmm why?")']
'''
