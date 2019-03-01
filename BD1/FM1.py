from __future__ import print_function
import re
import hashlib
import mmh3
MM = ['mm']
OH = ['oh', 'ah']
SIGH = ['sigh', 'sighed', 'sighing', 'sighs', 'ugh', 'uh']
UM = ['umm', 'hmm', 'huh']
nonFluencyDictionary = {'mm':'MM','oh':'OH','ah':'OH','si':'SIGH', 'ug':'SIGH', 'uh':'SIGH','um':'UM', 'hm':'UM', 'hu':'UM'}
distinctPhraseCounts = {'MM': 0,
                            'OH': 0,
                            'SIGH': 0,
                            'UM': 0}
#^.*\b((?i)ugh+|(?i)mm+|(?i)oh+|(?i)ah+|(?i)um+|(?i)hm+|(?i)huh+|(?i)ugh+|(?i)uh+|(?i)sigh+)(.)*\b.*$
def returnMatch(line):
    group = re.search(r'^.*\b(ugh+|mm+|oh+|ah+|um+|hm+|huh+|ugh+|uh+|sigh+)(.)*\b.*$',str(line),0|re.I)
    if (group):
        return [group.group(1),line.split(group.group(1))[1].strip()];  # [line[0],line[1].split(group.group(1))[1]]

data = [['The Big Orange, CA', 'Ugh lost ma keys last night'], ['Stony Brook, NY', 'Ugh lost my keys last night'], ['Springfield, IL', 'New AWS tool, Data Bricks Ummmmmm why?']]

#print(data[0][1][0]) #Extracts the post alone
#binArray = [[0 for i in range(127)] for i in range(3)]
binArray = [0 for i in range(1000)]

def hashMD5(word):
    sum = 0;
    hashValue = 0;
    hv = hashlib.md5(word.encode())
    for i in hv.hexdigest():
        sum = sum + ord(i)
    hashValue = sum%13;
    return hashValue;

def maxi(a,b):
    if(a>b):
        return a;
    else:
        return b;

def trail(hv):
    n = 0;
    if hv==0:
        return 0;
    else:
        while((hv >> 1) & 1 ==0):
            n = n+1;
        return n;
def FM(word):
    h1 = hashMD5(word)
    print(h1)
    return trail(h1)

max = 0
for d in data:
    list = (returnMatch(d[1]))
    print(list)
    wordToBeHashed = ""
    if (list[1]!= None):
        words = list[1].replace(".","").replace("!","").split(" ")
        for word in words[:3]:
            if(word):
                wordToBeHashed += word;
        cmax = FM(wordToBeHashed)
        max = maxi(max,cmax)
print(2**max)