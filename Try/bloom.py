from __future__ import print_function

from NewBloom import BloomFilter

from pyspark import SparkContext

MM = ['mm']
OH = ['oh', 'ah']
SIGH = ['sigh', 'sighed', 'sighing', 'sighs', 'ugh', 'uh']
UM = ['umm', 'hmm', 'huh']

sc = SparkContext(appName="PythonStreamingNetworkWordCount")
rdd = sc.textFile('C://Users//anand//Downloads//convertcsv.csv').map(lambda inp: inp.replace('"', "")).map(lambda x : x)
def trackLocations(loc):
    b.add("Hi")

b = BloomFilter("C://Users//anand//Documents//PythonProjects//data.txt",1000,0.001,True)
rdd.foreach(trackLocations)
print(b.__contains__("Hi"))


#b.add("Stony Brook, NY")
print(b.__contains__("Hi"))
#print(f'{b.hash_count}')
