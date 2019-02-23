nnR = [((1, 1), [('A', 1, 1), ('A', 2, 2), ('B', 1, 1), ('B', 2, 2)]),
 ((2, 1), [('B', 1, 1), ('B', 2, 2), ('A', 1, 2), ('A', 2, 1)])]
def take3(val):
    return val[1];
for k,v in nnR:
    A = []
    B = []
    print(f'{k} --> {v}')
    for val in v:
        #print(val[2])
        if(val[0]=='A'):
            A.append(val)
        else:
            B.append(val)
    A.sort(key=take3)
    B.sort(key=take3)
    print(f'Sorted - {A} -- {B}')
    sum = 0;
    for a,b in zip(A,B):
        sum = sum + a[2]*b[2];
    print(f'{sum}')
#print(f'{A} -- {B}')

#print(f'Sorted - {A} -- {B}')

d = [('A', 1, 2), ('A', 2, 1), ('B', 1, 1), ('B', 2, 2)]
print(type(ord('A')))

d = (4,2)
print(d[0])