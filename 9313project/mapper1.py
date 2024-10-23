#!/usr/bin/python3
import sys
import re 


for line in sys.stdin:
    
    line = line.lower()
    line = line.strip()
    words = line.split()

    for word in words: 
        if word[0].isalpha():
            print(word[0])

    