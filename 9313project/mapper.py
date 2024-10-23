#!/usr/bin/python3

from collections import defaultdict
import re
import sys

# this is combiner
tmp = {}

for line in sys.stdin:
   line = line.strip()
   words = re.split("[ *$&#/\t\n\f\"\'\\,.:;?!\[\](){}<>~\-_]", line.lower())

   for i in range(0, len(words)):
      tmp[words[i]] = {}
      for j in range(0, len(words)):
         if words[j] != words[i]:
            if words[j] not in tmp[words[i]]:  
               tmp[words[i]][words[j]] = 1
            else:
               tmp[words[i]][words[j]] += 1
               # tmp[words[i]] = tmp.get(words[j], 0) + 1
         
for i, j in tmp.items():
   print(i, j)
      




