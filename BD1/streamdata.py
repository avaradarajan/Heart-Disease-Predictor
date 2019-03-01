from __future__ import print_function

import sys
import re
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

MM = ['mm']
OH = ['oh', 'ah']
SIGH = ['sigh', 'sighed', 'sighing', 'sighs', 'ugh', 'uh']
UM = ['umm', 'hmm', 'huh']

def checkMatch(line):

    group = (re.search(r'^.*\b((?i)ugh+|(?i)mm+|(?i)oh+|(?i)ah+|(?i)um+|(?i)hm+|(?i)huh+|(?i)ugh+|(?i)uh+|(?i)sigh+)(.)*\b.*$',str(line)))
    if(group):
        return line.split(group.group(1))[1];
    else:
        return "No Match"
def checkE(loc):
    print(f'Location -> {loc}')

sc = SparkContext(appName="PythonStreamingNetworkWordCount")
ssc = StreamingContext(sc, 3)
rdd = sc.textFile('C://Users//anand//Downloads//convertcsv.csv').map(lambda inp: inp.replace('"', ""))
#rdd.foreach(checkE)

lines = ssc.socketTextStream("localhost",9999)
w = lines.map(lambda x: x)#.replace(".",'').repl).map(lambda x:(x.split(',"')[0],checkMatch(x.split(',"')[1])))
w.pprint()
ssc.start()
ssc.awaitTermination()

#works ^.*\b((?i)ugh+|(?i)mm+|(?i)oh+|(?i)ah+|(?i)um+|(?i)hm+|(?i)huh+|(?i)ugh+|(?i)uh+|(?i)sigh+)\b.*$


#^[\w\s]*(sigh|oh|mm|um)+[\w\s]*$

#^[\@\.\/\#\&\+\-\w\d\s]*[(ugh+)]+[\@\.\/\#\&\+\-\w\d\s]*$

#^[\@\.\/\#\&\+\-\w\d\s]*[(ugh+|mm+|oh+|ah+|um+|hm+|huh+|ugh+|uh+|sigh+)]+[\@\.\/\#\&\+\-\w\d\s]*$