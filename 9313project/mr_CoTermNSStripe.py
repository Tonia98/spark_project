import re
from mrjob.job import MRJob

class CoTermNSStripe(MRJob):

    def mapper_init(self):
        self.tmp = {}

    def mapper(self, _, line):
        line = line.strip()
        words = re.split("[ *$&#/\t\n\f\"\'\\,.:;?!\[\](){}<>~\-_]", line.lower())
        words = words.replace(" ", "")
        for i in range(0, len(words)):  
            self.tmp[words[i]] = {}
            for j in range(0, len(words)):
                if words[j] != words[i]:
                    if words[j] in self.tmp[words[i]]:  
                        self.tmp[words[i]][words[j]] += 1
                    else:
                        self.tmp[words[i]][words[j]] = 1

    def mapper_final(self):
        for i, j in self.tmp.items():
            yield i, j
 
    
    def reducer(self, key, values):
        result = {}
        for value in values:

            for word, fre in value.items():
                result[word] = result.get(word, 0) + int(fre)

        for w, v in result.items():
            yield key + " " + str(w), str(v)   


if __name__ == '__main__':
    CoTermNSStripe.run()

