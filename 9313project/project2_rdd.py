from pyspark import SparkContext, SparkConf
import sys, re
import itertools

# We will use the following command to run your code: $ spark-submit project2_rdd.py input output stopwords k

class Project2:
    def run(self, inputPath, outputPath, stopwordsPath, k): 
        conf = SparkConf().setAppName("project2")
        sc = SparkContext(conf=conf)

        textfile = sc.textFile(inputPath)
        stopwords = set(sc.textFile(stopwordsPath).collect())
        words = textfile.map(lambda line: line.split(",")).map(lambda parts: (parts[0][:4], [word for word in parts[1].split(" ") if word not in stopwords]))
        words = words.map(lambda line: (line[0], [word for word in line[1] if re.match(r'^[a-z]', word)]))
        
        '''
        print(words.collect())
        [('2003', ['council', 'chief', 'executive', 'fails', 'secure', 'position']), 
        ('2003', ['council', 'welcomes', 'ambulance', 'levy', 'decision']), 
        ('2003', ['council', 'welcomes', 'insurance', 'breakthrough']), 
        ('2003', ['fed', 'opp', 'introduce', 'national', 'insurance']), 
        ('2004', ['cowboys', 'survive', 'eels', 'comeback']), 
        ('2004', ['cowboys', 'withstand', 'eels', 'fightback']), 
        ('2004', ['castro', 'vows', 'cuban', 'socialism', 'survive', 'bush']), 
        ('2020', ['coronanomics', 'learnt', 'coronavirus', 'economy']), 
        ('2020', ['coronavirus', 'home', 'test', 'kits', 'selling', 'chinese', 'community']), 
        ('2020', ['coronavirus', 'campbell', 'remess', 'streams', 'bear', 'classes']), 
        ('2020', ['coronavirus', 'pacific', 'economy', 'foriegn', 'aid', 'china']), 
        ('2020', ['china', 'builds', 'pig', 'apartment', 'blocks', 'guard', 'swine', 'flu'])]
        '''
        pairs = words.flatMap(lambda pair: [((pair[0], combination)) for combination in itertools.combinations(pair[1], 2)])
        '''
        print(pairs.collect())
        example for first sentence
        ('2003', ['council', 'chief', 'executive', 'fails', 'secure', 'position'])
        =>
        here are C(6,2) = 15 kinds of pairs.
        
        ('2003', ('council', 'chief')), 
        ('2003', ('council', 'executive')), 
        ('2003', ('council', 'fails')), 
        ('2003', ('council', 'secure')), 
        ('2003', ('council', 'position')), 
        ('2003', ('chief', 'executive')), 
        ('2003', ('chief', 'fails')), 
        ('2003', ('chief', 'secure')), 
        ('2003', ('chief', 'position')), 
        ('2003', ('executive', 'fails')),
        ('2003', ('executive', 'secure')), 
        ('2003', ('executive', 'position')), 
        ('2003', ('fails', 'secure')), 
        ('2003', ('fails', 'position')),
        ('2003', ('secure', 'position')),

        '''
        count = pairs.map(lambda pair: (pair, 1))
        '''
        provide value 1 for each pair
        example for first sentence
        print(count.collect())
        (('2003', ('council', 'chief')), 1), 
        (('2003', ('council', 'executive')), 1), 
        (('2003', ('council', 'fails')), 1), 
        (('2003', ('council', 'secure')), 1), 
        (('2003', ('council', 'position')), 1), 
        (('2003', ('chief', 'executive')), 1), 
        (('2003', ('chief', 'fails')), 1), 
        (('2003', ('chief', 'secure')), 1), 
        (('2003', ('chief', 'position')), 1), 
        (('2003', ('executive', 'fails')), 1), 
        (('2003', ('executive', 'secure')), 1), 
        (('2003', ('executive', 'position')), 1), 
        (('2003', ('fails', 'secure')), 1), 
        (('2003', ('fails', 'position')), 1), 
        (('2003', ('secure', 'position')), 1), 
        '''
        count = count.map(lambda x: ((x[0][0], tuple(sorted(x[0][1]))), x[1]))
        '''
        sorting the key of the count to treat (u,w) and (w,u) as the same key 
        '''
        counts = count.reduceByKey(lambda a, b: a + b)
        
        '''
        By incrementing the value of the counter, 
        we can keep track of the repeated occurrences of events 
        and update the counter value as new data emerges.
        
        print(counts.collect())
        (('2004', ('cowboys', 'eels')), 1)
        (('2004', ('cowboys', 'eels')), 1)
        =>
        (('2004', ('cowboys', 'eels')), 2)

        '''
        sortCount = counts.sortBy(lambda x: (x[0][0], -x[1], x[0][1][0], x[0][1][1]))
        for i in sortCount.collect():
            print(i)
        
        '''
        understand what counts is first:
        counts = (('2004', ('cowboys', 'eels')), 2)
                   x[0][0]   x[0][1][0]   x[0][1][1]  x[1]
        sorting the results according to year x[0][0], then counts x[1], then by terms in alphabetical order.

        for i in sortCount.collect():
            print(i)
        (('2003', ('council', 'welcomes')), 2)
        (('2003', ('ambulance', 'decision')), 1)
        (('2003', ('ambulance', 'levy')), 1)
        (('2003', ('chief', 'executive')), 1)
        (('2003', ('chief', 'fails')), 1)
        (('2003', ('chief', 'position')), 1)
        (('2003', ('chief', 'secure')), 1)

        '''
        group = sortCount.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey().sortByKey().mapValues(list)
        result = group.map(lambda r: (r[0],r[1][:k]))
        
        '''
        Transform tuple structure, 
        Group tuples by year as a key, converting to lists.
        For each group, keep top 'k' elements 

        print(result.collect())
        the result when k = 3
        [('2003', [(('council', 'welcomes'), 2), (('ambulance', 'decision'), 1), (('ambulance', 'levy'), 1)]), 
        ('2004', [(('cowboys', 'eels'), 2), (('castro', 'bush'), 1), (('castro', 'cuban'), 1)]), 
        ('2020', [(('coronavirus', 'economy'), 2), (('aid', 'china'), 1), (('apartment', 'blocks'), 1)])]

        '''  
        output = result.flatMap(lambda x: [(x[0], i) for i in x[1]]).map(lambda x: f"{x[0]}    {x[1][0][0]},{x[1][0][1]}:{x[1][1]}")
        '''
        formatting the result
        '''
        # for i in output.collect():
        #     print(i)

        output.coalesce(1).saveAsTextFile(outputPath)
        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong inputs")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]))