import hashlib
import binascii
words = ['lost','my','keys']
binArray = [[0 for i in range(127)] for i in range(3)]
#print(len(binArray))
for word in words:
    sum = 0;
    hv = hashlib.md5(word.encode())
    for i in hv.hexdigest():
        sum = sum + ord(i)
    binArray[0][sum%127] = 1
    #print(f'SUM H1 - {sum%127}')

for word in words:
    sum = 0;
    hv = hashlib.sha3_256(word.encode())
    for i in hv.hexdigest():
        sum = sum + ord(i)
    binArray[1][sum%127] = 1
    #print(f'SUM H2 - {sum % 127}')

for word in words:
    sum = 0;
    hv = hashlib.sha3_512(word.encode())
    for i in hv.hexdigest():
        sum = sum + ord(i)
    binArray[2][sum%127] = 1
    #print(f'SUM H3 - {sum % 127}')

def contains(word):
    hv = hashlib.md5(word.encode())
    h1 = True;
    h2 = True;
    h3 = True;
    sum=0
    for i in hv.hexdigest():
        sum = sum + ord(i)
    #print(sum%127)
    if(binArray[0][sum%127] != 1):
       h1 = False;
    else:
        return h1;

    hv = hashlib.sha3_256(word.encode())
    sum = 0
    for i in hv.hexdigest():
        sum = sum + ord(i)
    #print(sum % 127)
    if(binArray[1][sum%127] != 1):
        h2 = False;
    else:
        return h2;

    hv = hashlib.sha3_512(word.encode())
    sum = 0
    for i in hv.hexdigest():
        sum = sum + ord(i)
    #print(sum % 127)
    if(binArray[2][sum%127] != 1):
            h3 = False;
    else:
        return h3;
    return h1 and h2 and h3;

print(contains("lost"))
print(contains("my"))
print(contains("keys"))
print(contains("ym"))
print(contains("key"))
print(contains("stol"))

#for i in binArray:
#    print(i);
#binary = lambda x: "".join(reversed([i + j for i, j in zip(*[["{0:04b}".format(int(c, 16)) for c in reversed("0" + x)][n::2] for n in [1, 0]])]))
#print(int(binary(hv.hexdigest()),2))
