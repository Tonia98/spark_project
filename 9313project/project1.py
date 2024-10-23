from collections import defaultdict
import re
import math
from mrjob.job import MRJob
from mrjob.protocol import ujson
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env

class proj1(MRJob):   

    def mapper(self, _, line):
        # Extracts the year from the line and splits the line into words
        year = line[:4]
        parts = line.split(",")
        words = re.split(r'\t|,|; |\s', parts[1])
        for w in words:     
            # Yields a key-value pair of word and year with a count of 1
            yield (w, year), 1
            
    def reducer_init(self):
        # Initializes dictionaries to store unique years per term and term frequency
        self.term_dict = defaultdict(set)
        self.term_count = defaultdict(int)
       
    def reducer(self, key, freqs):
        # Sums up the counts for each word and year
        key_tuple = tuple(key)
        self.term_count[key_tuple] = sum(freqs)
        
        # Order inversion (Inverts the order to term and year):
        # In this program, Order Inversion is used to first flip 
        # the order of words (term) and years (year) and add the 
        # year to the set of corresponding words before processing
        # the other values (in this case, counts). This allows the
        # program to first process the set of total counts and years 
        # for all words before performing the IDF calculation.
        term, year = key
        self.term_dict[term].add(year)
            
    def reducer_final(self):
        # Retrieves the beta value and total number of years from the environment variables
        # beta = float(jobconf_from_env('myjob.settings.beta'))
        # n = int(jobconf_from_env('myjob.settings.years'))

        # Secondary Sort:
        # This step is using secondary sort based on the key_tuple in term_count dictionary.
        # it first sorts based on the first key (word) and then sorts in descending order based
        # on the year.
        term_count_copy = dict(sorted(
            self.term_count.items(),
            key=lambda x: (x[0][0], -int(x[0][1]))
        ))
        
        # Creates a copy of the term_count dictionary for iteration
        for term_tuple, val in term_count_copy.items():
            term = term_tuple[0]
            year = term_tuple[1]
          
            # Calculates IDF and checks if term weight is greater than or equal to beta
            idf = math.log10(3 / len(self.term_dict[term]))
            weight = self.term_count[term_tuple] * idf
            if weight >= 0.4:
                yield term, year + ',' + str(weight)
    
    SORT_VALUES = True
    def steps(self):
        # Return the steps of the MapReduce job
        return [
            MRStep(mapper=self.mapper, reducer_init=self.reducer_init, reducer=self.reducer, reducer_final=self.reducer_final)
        ]

if __name__ == '__main__':
    proj1.run()
