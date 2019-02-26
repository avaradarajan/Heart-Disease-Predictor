from __future__ import print_function
import re
import hashlib
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
    hash1 = False;
    sum = 0;
    hashValue = 0;
    hv = hashlib.md5(word.encode())
    for i in hv.hexdigest():
        sum = sum + ord(i)
    hashValue = sum%1000;
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
    hashValue = sum % 1000;
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
    hashValue = sum % 1000;
    if (binArray[hashValue] == 1):
        hash3 = True;
    else:
        hash3 = False;
        binArray[hashValue] = 1;
    return hash3;

for d in data:
    list = (returnMatch(d[1]))
    print(list)
    wordToBeHashed = ""
    if (list[1]!= None):
        words = list[1].replace(".","").replace("!","").split(" ")
        for word in words[:3]:
            if(word):
                wordToBeHashed += word;
            h1 = hashMD5(wordToBeHashed)
            h2 = hashSHA256(wordToBeHashed)
            h3 = hashSHA512(wordToBeHashed)
        if(not (h1 and h2 and h3)):
            if (list != None and nonFluencyDictionary.get(list[0][:2].lower())):
                distinctPhraseCounts[nonFluencyDictionary[list[0][:2].lower()]] = distinctPhraseCounts[nonFluencyDictionary[list[0][:2].lower()]] + 1;
        print(f'{nonFluencyDictionary.get(list[0][:2].lower())} <--> {distinctPhraseCounts[nonFluencyDictionary[list[0][:2].lower()]]}')
