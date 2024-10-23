#!/usr/bin/python3
import sys
import re

results = {}
for line in sys.stdin:
    line = line.replace("{", "")
    line = line.replace("}\n", "")
    all_words = line.split()
    first_word = all_words[0]
    t = line.split(",")
    for element in t:
        d = re.findall(r"'([^']*)'", element)
        fre = element.split(":", 1)[-1].replace(" ", "")
        # print(fre)
        # print(d[0])
        e = first_word + " " + d[0]
        # print(element)
        results[e] = results.get(e,0) + int(fre)
    
        # print(results)
    
for k,v in results.items():
    print(k,v)





# the {'number': 1, 'of': 1, 'tts': 1, 'in': 1, 'words': 1}
# number {'the': 2, 'of': 1, 'tts': 1, 'in': 1, 'words': 1}
# of {'the': 2, 'number': 1, 'tts': 1, 'in': 1, 'words': 1}
# tts {'the': 2, 'number': 1, 'of': 1, 'in': 1, 'words': 1}
# in {'the': 2, 'number': 1, 'of': 1, 'tts': 1, 'words': 1}
# words {'the': 2, 'number': 1, 'of': 1, 'tts': 1, 'in': 1}