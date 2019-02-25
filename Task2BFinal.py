from __future__ import print_function

import sys
import re
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

MM = ['mm']
OH = ['oh', 'ah']
SIGH = ['sigh', 'sighed', 'sighing', 'sighs', 'ugh', 'uh']
UM = ['umm', 'hmm', 'huh']

sc = SparkContext(appName="PythonStreamingNetworkWordCount")
ds = sc.parallelize([("Stony Brook, NY","Ugh. lost my keys last night"),("Somewhere someplace","Ugh. lost my keys last night"),("Stony Brook, NY","lost my keys last night"),("Springfield, IL","New AWS tool, Data Bricks. Ummmmmm why?")])

rdd = sc.textFile('C://Users//anand//Downloads//convertcsv.csv').map(lambda inp: inp.replace('"', "")).map(lambda x : (x,''))
#mp = tuple(rdd.collect(),'0')
#print(mp)
#rdd.foreach(checkE)

def checkMatch(line):
    group = (re.search(r'^.*\b((?i)ugh+|(?i)mm+|(?i)oh+|(?i)ah+|(?i)um+|(?i)hm+|(?i)huh+|(?i)ugh+|(?i)uh+|(?i)sigh+)(.)*\b.*$',str(line[1])))
    if(group):
        return [line[0],line[1]]; #[line[0],line[1].split(group.group(1))[1]]
def checkE(loc):
    print(f'Location -> {loc[0]}---{loc[1]}--{loc[1][0]}')
def returnMatch(line):
    group = (re.search(r'^.*\b((?i)ugh+|(?i)mm+|(?i)oh+|(?i)ah+|(?i)um+|(?i)hm+|(?i)huh+|(?i)ugh+|(?i)uh+|(?i)sigh+)(.)*\b.*$',str(line[1])))
    if(group):
        return [line[0],line[1]]; #[line[0],line[1].split(group.group(1))[1]]


w = ds.map(lambda x: list(x)).map(lambda x: checkMatch(x)).filter(lambda x : x!=None).map(lambda x : (x[0],str(x[1]).replace(".","").strip()))
print(w.collect())
#[['Stony Brook, NY', 'lost my keys last night'], ['Somewhere someplace', 'lost my keys last night'], ['Springfield, IL', 'why?']]
#Step 1 completed - Considering only non fluencies posts

#step2 - Consider only valid locations
#st = w.map(lambda x : checkLocation(x)).filter(lambda x : x!=None)
#print(st.collect())

fd = w.join(rdd)
#print(fd.collect())
#[('Stony Brook, NY', ('lost my keys last night', '')), ('Springfield, IL', ('why?', ''))]
#[('Stony Brook, NY', ('Ugh lost my keys last night', '')), ('Springfield, IL', ('New AWS tool, Data Bricks Ummmmmm why?', ''))]
#Successfully collected all data which match location and post criteria

#Step 3 - Count distinct elements for a group

def countDist(loc):
    print(f'Location -> {loc[0]}- Second list -{loc[1]}- Actual Post - {loc[1][0]}')
    return;

fd.foreach(countDist)
'''
def checkE(loc):
    print(f'Location -> {loc[0]}---{loc[1]}--{loc[1][0]}')
'''