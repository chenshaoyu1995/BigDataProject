import sys

row = (int)(sys.argv[1])
col = (int)(sys.argv[2])

for line in sys.stdin:
    if row <= 0:
        break
    line = line.strip('\n')
    tmp = line.split(',')

    first = True
    res = ''
    for i in range(col):
        if not first:
            res += ','
        res += tmp[i]
        first = False
    print (res)
 

