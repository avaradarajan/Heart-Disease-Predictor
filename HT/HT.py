import json, operator
from pyspark import SparkContext, SparkConf
import csv
import numpy as np
import pprint
from scipy import stats
from numpy.linalg import pinv
import sklearn.preprocessing as sp
import sklearn.linear_model as lr
np.seterr(divide='ignore', invalid='ignore')
def wordList(x):
    listt = []
    for val in x:
        listt.append((val[0][1],[val[0][0],val[0][2],val[1][1],val[1][2]]))
    return listt


def wordGroup(x):
    listt = []
    for val in x[1]:
        listt.append(val)
    return (x[0],listt)

def computeLinearRegression(wdata):
    listval = []
    for vals in wdata[1]:
        listval.append([float(vals[1]),float(vals[2])])
    xy = np.array(listval)
    scaler = sp.StandardScaler()
    scaled = scaler.fit_transform(xy)
    xlist = []
    for vl in scaled[:,0]:
        xlist.append([1.,float(vl)])
    x = np.matrix(xlist)
    y = np.matrix(scaled[:,1]).transpose()
    invs = pinv(np.matmul(np.transpose(x),x))
    xty = np.matmul(np.transpose(x),y)
    ans = np.matmul(invs,xty)
    coeff = np.array(ans[1]).flatten().tolist()[0]
    yarr = np.array(scaled[:,1])
    xarr = np.array(scaled[:, 0])
    '''e = np.sum((yarr - np.array(ans[1]).flatten().tolist()[0])**2)
    #I don't have to subtract with mean. Cos its standardized
    denom = np.sum((xarr - np.mean(xarr))**2)
    with np.errstate(divide='ignore', invalid='ignore'):
        se = np.divide((e / 18), denom)
        t = np.nan_to_num(np.divide(coeff, se))
    print("++")
    print(t)
    print("++")
    print(stats.t.ppf(1-0.025,18))
    print("--")
    pvalue = stats.t.sf(np.abs(t),18)*2
    print(pvalue)'''
    return (wdata[0],np.array(ans[1]).flatten().tolist()[0],xarr,yarr);



def printLines(sc,w,h):
    headerw = w.first()
    headerh = h.first()
    wrdd = w.filter(lambda x: x!=headerw).map(lambda x: (x[0],[x[0],x[1],x[3]]))
    hrdd = h.filter(lambda x: x!=headerh).map(lambda x: (x[0],[x[0],x[24],x[23]]))
    joinedCounty = wrdd.join(hrdd).groupByKey().flatMap(lambda x: wordList(x[1])).groupByKey().map(lambda x: wordGroup(x))#.reduceByKey(lambda x,y: callfu(x,y))#.map(lambda x: (x[0][1],[x[0][0],x[0][2],x[1][1],x[1][2]]))#map(lambda x: (x[1][0][0],x[1][0][1],x[1][0][2],x[1][1][1],x[1][1][2]))#.reduceByKey(lambda x,y: (xx[0][0],x[0][1],x[0][2],y[0][1],y[0][2]))
    #joinedCounty.foreach(print)
    withoutControlRDD = joinedCounty.map(lambda x: computeLinearRegression(x))#.map(lambda x:x[1])#
    top20wc = withoutControlRDD.sortBy(ascending=False,keyfunc=lambda x : x[1]).take(20)
    last20wc =withoutControlRDD.sortBy(ascending=True,keyfunc=lambda x : x[1]).take(20)
    print("-------------------------------------------------------------------------")
    #withoutControlRDD.foreach(print)
    print(top20wc)
    print(last20wc)
    for wordData in top20wc:
        e = np.sum((wordData[3] - wordData[1]) ** 2)
        # I don't have to subtract with mean. Cos its standardized
        denom = np.sum((wordData[3] - np.mean(wordData[3])) ** 2)
        with np.errstate(divide='ignore', invalid='ignore'):
            se = np.divide((e / 18), denom)
            t = np.nan_to_num(np.divide(wordData[1], se))
        print("++")
        #print(t)
        print("++")
        #print(stats.t.ppf(1 - 0.025, 18))
        print("--")
        pvalue = stats.t.sf(np.abs(t), 18) * 2
        print(wordData[0],pvalue)
    '''e = np.sum((yarr - np.array(ans[1]).flatten().tolist()[0])**2)
    #I don't have to subtract with mean. Cos its standardized
    denom = np.sum((xarr - np.mean(xarr))**2)
    with np.errstate(divide='ignore', invalid='ignore'):
        se = np.divide((e / 18), denom)
        t = np.nan_to_num(np.divide(coeff, se))
    print("++")
    print(t)
    print("++")
    print(stats.t.ppf(1-0.025,18))
    print("--")
    pvalue = stats.t.sf(np.abs(t),18)*2
    print(pvalue)'''

    #compute t-value
    #compute se

    #joinedCounty.take(5)
    #print(hrdd.take(5))

if __name__ == "__main__":
    print("Starting to parse files")
    conf = SparkConf().setMaster("local").setAppName("HT")
    sc = SparkContext(conf=conf)
    wordData = sc.textFile('C://Users//anand//Documents//countyoutcomes//dummy//wcpy.csv').mapPartitions(lambda line: csv.reader(line))
    heartData = sc.textFile('C://Users//anand//Documents//countyoutcomes//dummy//ccpy.csv').mapPartitions(lambda line: csv.reader(line))
    #wordData = sc.textFile('C://Users//anand//Documents//countyoutcomes//word_sample1.csv').mapPartitions(lambda line: csv.reader(line))
    #eartData = sc.textFile('C://Users//anand//Documents//countyoutcomes//countyoutcomes.csv').mapPartitions(lambda line: csv.reader(line))
    #wordData = sc.textFile('C://Users//anand//Documents//countyoutcomes//dummy//*.csv').mapPartitions(lambda line: csv.reader(line))
    printLines(sc,wordData,heartData)