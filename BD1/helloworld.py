import re
s = "That bothered huh"
group = (re.search(
    r'^.*\b(mm+|oh+|ah+|umm*|hmm*|huh|ugh|uh|sigh+)\b.*$', s, 0 | re.I))

l = re.findall(r'\b(mm+|oh+|ah+|umm*|hmm*|huh|ugh|uh|sigh+)\b',s)
for reg in l:
    print(reg)

lst = []

currentList = []
'''for m in re.finditer(r'\b(mm+|oh+|ah+|umm*|hmm*|huh|ugh|uh|sigh+)\b', s,0|re.I):
    word = s[m.start():].split(" ");
    group = s[m.start():m.end()]
    final = ""
    try:
        word[1]
        for val in word[1:4]:
            final += val
        currentList.append([group, final.strip()])
        print(f'{s} {currentList}')
    except IndexError:
        print("Hi")

    print(f'{s} {currentList}')'''
sd = ""
if sd:
    print(Hi)

#print(l)

#print([(a.start(), a.end()) for a in list(re.finditer(group.group(1), s))])
#print(s.split(group.group(1))[1][s.split(group.group(1))[1].find(" "):].strip())
#print(s.split(group.group(1))[1][s.split(group.group(1))[1].find(" "):].strip())
