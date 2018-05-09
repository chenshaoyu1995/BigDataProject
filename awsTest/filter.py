import sys
start = False
for line in sys.stdin:
    line = line.strip('\n')
    if line == 'resultStartLine':
        start = True
    elif line == 'resultEndLine':
        start = False
    elif start:
        print (line)
