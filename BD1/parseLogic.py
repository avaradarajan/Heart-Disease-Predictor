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

def hashMD5(word):
    sum = 0;
    hv = hashlib.md5(word.encode())
    for i in hv.hexdigest():
        sum = sum + ord(i)
    binArray[0][sum % 127] = 1
for d in data:
    list = (returnMatch(d[1][0]))
    if (list!= None and nonFluencyDictionary.get(list[0][:2].lower())):
        distinctPhraseCounts[nonFluencyDictionary[list[0][:2].lower()]] = distinctPhraseCounts[nonFluencyDictionary[list[0][:2].lower()]] + 1;
        print(distinctPhraseCounts[nonFluencyDictionary[list[0][:2].lower()]])
        print(list)