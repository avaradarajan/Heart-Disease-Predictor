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

data = [('Stony Brook, NY', ('ughhh lost my keys last night', '')), ('Springfield, IL', ('New AWS tool, Data Bricks Umm. why?', ''))]

#print(data[0][1][0]) #Extracts the post alone
binArray = [[0 for i in range(127)] for i in range(3)]
def hashMD5(word):
    hash1 = False;
    sum = 0;
    hashValue = 0;
    hv = hashlib.md5(word.encode())
    for i in hv.hexdigest():
        sum = sum + ord(i)
    hashValue = sum%127;
    #if(binArray[0][sum % 127]==1):
    #    hash1 =
    #binArray[0][sum % 127] = 1
def hashSHA256(word):
    sum = 0;
    hashValue = 0;
    hv = hashlib.sha3_256(word.encode())
    for i in hv.hexdigest():
        sum = sum + ord(i)
    binArray[0][sum % 127] = 1
def hashSHA512(word):
    sum = 0;
    hashValue = 0;
    hv = hashlib.sha3_256(word.encode())
    for i in hv.hexdigest():
        sum = sum + ord(i)
    binArray[0][sum % 127] = 1

for d in data:
    list = (returnMatch(d[1][0]))
    if (list[1]!= None):
        words = list[1].split(" ")
        for word in words[:3]:
            if(word):
                if(len(word)==1):
                    if(ord(word)):
                        hashMD5(word)
                        hashSHA256(word)
                        hashSHA512(word)
                else:
                    hashMD5(word)
                    hashSHA256(word)
                    hashSHA512(word)