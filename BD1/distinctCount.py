from __future__ import print_function
import re
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

data = [('Stony Brook, NY', ('ughhh lost my keys last night', '')), ('Springfield, IL', ('New AWS tool, Data Bricks Ummmmmmm why?', ''))]

#print(data[0][1][0]) #Extracts the post alone

for d in data:
    list = (returnMatch(d[1][0]))
    if (list[1]!= None):
        words = list[1].split(" ").append("")
        #for i in words[:3]:
        print(f'{words[0]} {words[1]} {words[2]}')
        #print(list[1][len(list[0]):])