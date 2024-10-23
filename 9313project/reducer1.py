#!/usr/bin/python3
import sys

result = {}
for line in sys.stdin: 
    line = line.strip()
    if line in result:
        result[line] += 1
    else: 
        result[line] = 1

    
for i in result.keys():
    print(i, result[i])


