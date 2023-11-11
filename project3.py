import sys 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, sort_array, explode, lit, monotonically_increasing_id, collect_list, asc, udf, concat, round
from pyspark.sql.types import ArrayType, StringType, FloatType
from pyspark.sql import functions as F

# The program take three parameters: 
# So, the terminal command will be 
# $ spark-submit project3.py input output tau

class Project3: 
    def token_ordering(self,df):
        df_token = df.select(explode("words").alias("word"))
        df_token = df_token.withColumn("count", lit(1))
        df_token = df_token.groupBy("word").count().withColumnRenamed("count", "total_count")
        df_token = df_token.orderBy("total_count", "word")
        return df_token
    
    def sorted_df(self,token_df,df):
        sorted_df = df.select("id", "year", explode("words").alias("word")).join(token_df, "word").orderBy(asc("total_count"), "word").groupby("id", "year").agg(collect_list("word").alias("sorted_words"))
        sorted_df = sorted_df.orderBy("id")
        return sorted_df
    
    def pairs_generation(self, sorted_df, tau):

        def prefix_length(words, tau):
            return words[:int(len(words) - tau * len(words) + 1)]
        
        prefix_length_udf = udf(lambda words: prefix_length(words, tau), ArrayType(StringType()))
        pairs_df = sorted_df.withColumn("prefix_tokens", prefix_length_udf("sorted_words"))
        pairs_df = pairs_df.select("id", "year", explode("prefix_tokens").alias("tokens"), "sorted_words")
        pairs_df = pairs_df.orderBy("tokens")
        return pairs_df

    def filter_pairs(self, filter_token, df):
        filter_df = df.join(filter_token, on="tokens", how="inner").select(df["*"]).orderBy("tokens")
        return filter_df

    def sim_pairs(self, df, tau):
        def similarity(words1, words2):
            set1, set2 = set(words1), set(words2)
            intersect = len(set1.intersection(set2))
            union = len(set1.union(set2))
            result1 = intersect / union
            return result1

        pairs_df = df.alias("df1").join(df.alias("df2"), (col("df1.tokens") == col("df2.tokens")) & (col("df1.id") < col("df2.id")) & (col("df1.year") != col("df2.year")))
        similarity_udf = udf(similarity, FloatType())
        result = pairs_df.withColumn("sim", similarity_udf("df1.sorted_words","df2.sorted_words"))
        result = result.select(col("df1.id").alias("id1"), col("df2.id").alias("id2"), col("sim"))
        result = result.orderBy("id1", "id2").filter(result.sim >= tau)

        return result

    def run(self, inputPath, outputPath, tau):

        spark = SparkSession.builder.master("local").appName("project3").getOrCreate()
        file = spark.read.text(inputPath)

        df = file.withColumn("id", monotonically_increasing_id())
        df = df.select(col("id"), col("value").substr(1,4).alias("year"), split(split(col("value"), ",")[1], " ").alias("words"))
        df = df.withColumn("words", sort_array(col("words")))
        '''
        Getting the table and make the headlines as an array of words 
        df.show(truncate=False)
        +---+----+------------------------------------------------------------+
        |id |year|words                                                       |
        +---+----+------------------------------------------------------------+
        |0  |2019|[adelaide, centre, shopping, stabbed, woman]                |
        |1  |2019|[continue, economy, edge, recession, teetering]             |
        |2  |2020|[coronanomics, coronavirus, economy, learnt]                |
        |3  |2020|[chinese, community, coronavirus, home, kits, selling, test]|
        |4  |2020|[aid, china, coronavirus, economy, foriegn, pacific]        |
        |5  |2020|[apartment, blocks, builds, china, flu, guard, pig, swine]  |
        |6  |2021|[bounce, economy, starts, unemployment]                     |
        |7  |2021|[coronavirus, due, online, rise, shopping]                  |
        |8  |2021|[china, close, elon, encounters, musks]                     |
        +---+----+------------------------------------------------------------+
        '''
        tokens = self.token_ordering(df)
        '''
        tokens.show(1000)
        Finding the frequency of each words 
        +------------+-----------+
        |        word|total_count|
        +------------+-----------+
        |    adelaide|          1|
        |         aid|          1|
        |   apartment|          1|
        |      blocks|          1|
        |      bounce|          1|
        |      builds|          1|
        |      centre|          1|
        |     chinese|          1|
        |       close|          1|
        |   community|          1|
        |    continue|          1|
        |coronanomics|          1|
        |         due|          1|
        |        edge|          1|
        |        elon|          1|
        |  encounters|          1|
        |         flu|          1|
        |     foriegn|          1|
        |       guard|          1|
        |        home|          1|
        |        kits|          1|
        |      learnt|          1|
        |       musks|          1|
        |      online|          1|
        |     pacific|          1|
        |         pig|          1|
        |   recession|          1|
        |        rise|          1|
        |     selling|          1|
        |     stabbed|          1|
        |      starts|          1|
        |       swine|          1|
        |   teetering|          1|
        |        test|          1|
        |unemployment|          1|
        |       woman|          1|
        |    shopping|          2|
        |       china|          3|
        | coronavirus|          4|
        |     economy|          4|
        +------------+-----------+
        '''
        sorted_df = self.sorted_df(tokens, df)
        '''
        sorted_df.show(truncate=False)
        Sorting the each word in a headline based on the frequncy table created above 
        +---+----+------------------------------------------------------------+
        |id |year|sorted_words                                                |
        +---+----+------------------------------------------------------------+
        |0  |2019|[adelaide, centre, stabbed, woman, shopping]                |
        |1  |2019|[continue, edge, recession, teetering, economy]             |
        |2  |2020|[coronanomics, learnt, coronavirus, economy]                |
        |3  |2020|[chinese, community, home, kits, selling, test, coronavirus]|
        |4  |2020|[aid, foriegn, pacific, china, coronavirus, economy]        |
        |5  |2020|[apartment, blocks, builds, flu, guard, pig, swine, china]  |
        |6  |2021|[bounce, starts, unemployment, economy]                     |
        |7  |2021|[due, online, rise, shopping, coronavirus]                  |
        |8  |2021|[close, elon, encounters, musks, china]                     |
        +---+----+------------------------------------------------------------+
        '''
        pairs_df = self.pairs_generation(sorted_df, tau)
        '''
        pairs_df.show(truncate=False)
        pairing each tokens with the headlines based onn the prefix length found. 
        +---+----+------------+------------------------------------------------------------+
        |id |year|tokens      |sorted_words                                                |
        +---+----+------------+------------------------------------------------------------+
        |0  |2019|adelaide    |[adelaide, centre, stabbed, woman, shopping]                |
        |4  |2020|aid         |[aid, foriegn, pacific, china, coronavirus, economy]        |
        |5  |2020|apartment   |[apartment, blocks, builds, flu, guard, pig, swine, china]  |
        |5  |2020|blocks      |[apartment, blocks, builds, flu, guard, pig, swine, china]  |
        |6  |2021|bounce      |[bounce, starts, unemployment, economy]                     |
        |5  |2020|builds      |[apartment, blocks, builds, flu, guard, pig, swine, china]  |
        |0  |2019|centre      |[adelaide, centre, stabbed, woman, shopping]                |
        |4  |2020|china       |[aid, foriegn, pacific, china, coronavirus, economy]        |
        |5  |2020|china       |[apartment, blocks, builds, flu, guard, pig, swine, china]  |
        |8  |2021|china       |[close, elon, encounters, musks, china]                     |
        |3  |2020|chinese     |[chinese, community, home, kits, selling, test, coronavirus]|
        |8  |2021|close       |[close, elon, encounters, musks, china]                     |
        |3  |2020|community   |[chinese, community, home, kits, selling, test, coronavirus]|
        |1  |2019|continue    |[continue, edge, recession, teetering, economy]             |
        |2  |2020|coronanomics|[coronanomics, learnt, coronavirus, economy]                |
        |3  |2020|coronavirus |[chinese, community, home, kits, selling, test, coronavirus]|
        |4  |2020|coronavirus |[aid, foriegn, pacific, china, coronavirus, economy]        |
        |2  |2020|coronavirus |[coronanomics, learnt, coronavirus, economy]                |
        |7  |2021|coronavirus |[due, online, rise, shopping, coronavirus]                  |
        |7  |2021|due         |[due, online, rise, shopping, coronavirus]                  |
        +---+----+------------+------------------------------------------------------------+
        ...
        '''
        filter_tokens = tokens.filter("total_count > 1").withColumnRenamed("word", "tokens")
        filter_pairs = self.filter_pairs(filter_tokens, pairs_df)
        '''
        filter_pairs.show(truncate=False)
        Filtering out all the tokens that only appear once because they would not have any similarity with other headlines  
        +---+----+-----------+------------------------------------------------------------+
        |id |year|tokens     |sorted_words                                                |
        +---+----+-----------+------------------------------------------------------------+
        |4  |2020|china      |[aid, foriegn, pacific, china, coronavirus, economy]        |
        |5  |2020|china      |[apartment, blocks, builds, flu, guard, pig, swine, china]  |
        |8  |2021|china      |[close, elon, encounters, musks, china]                     |
        |2  |2020|coronavirus|[coronanomics, learnt, coronavirus, economy]                |
        |3  |2020|coronavirus|[chinese, community, home, kits, selling, test, coronavirus]|
        |4  |2020|coronavirus|[aid, foriegn, pacific, china, coronavirus, economy]        |
        |7  |2021|coronavirus|[due, online, rise, shopping, coronavirus]                  |
        |1  |2019|economy    |[continue, edge, recession, teetering, economy]             |
        |2  |2020|economy    |[coronanomics, learnt, coronavirus, economy]                |
        |4  |2020|economy    |[aid, foriegn, pacific, china, coronavirus, economy]        |
        |6  |2021|economy    |[bounce, starts, unemployment, economy]                     |
        |0  |2019|shopping   |[adelaide, centre, stabbed, woman, shopping]                |
        |7  |2021|shopping   |[due, online, rise, shopping, coronavirus]                  |
        +---+----+-----------+------------------------------------------------------------+
        
        '''
        # doing self-join based on some condition and calculate sim
        sim_pairs = self.sim_pairs(filter_pairs, tau)
        filtered_sim_pairs = sim_pairs.dropDuplicates(["id1", "id2"])
        '''
        sim_pairs.show(truncate=False)
        Calculate the sim 
        +---+---+----------+
        |id1|id2|sim       |
        +---+---+----------+
        |0  |7  |0.11111111|
        |1  |2  |0.125     |
        |1  |4  |0.1       |
        |1  |6  |0.125     |
        |2  |6  |0.14285715|
        |2  |7  |0.125     |
        |4  |6  |0.11111111|
        |4  |7  |0.1       |
        |4  |8  |0.1       |
        +---+---+----------+
        '''
        # Formating the result
        output = filtered_sim_pairs.withColumn("output", concat(lit("("), col("id1"), lit(","), col("id2"), lit(")"), lit("    "), col("sim")))
        output = output.select("output")
        output.show(truncate=False)
        '''
        output.show(truncate=False)
        +-------------------+
        |output             |
        +-------------------+
        |(0,7)    0.11111111|
        |(1,2)    0.125     |
        |(1,4)    0.1       |
        |(1,6)    0.125     |
        |(2,6)    0.14285715|
        |(2,7)    0.125     |
        |(4,6)    0.11111111|
        |(4,7)    0.1       |
        |(4,8)    0.1       |
        +-------------------+
        '''
        output.coalesce(1).write.format("text").option("header", "true").save(outputPath)

        

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Wrong inputs")
        sys.exit(-1)
    Project3().run(sys.argv[1], sys.argv[2], float(sys.argv[3]))