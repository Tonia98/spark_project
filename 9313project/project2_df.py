import sys, itertools, re
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lit, row_number, concat, col, regexp_replace, split, array_sort, concat_ws
from pyspark.sql.types import StringType


class Project2:
    def run(self, inputPath, outputPath, stopwordsPath, k):         

        spark = SparkSession.builder.master("local").appName("project2").getOrCreate()
        file = spark.sparkContext.textFile(inputPath)
        stopwords = set(spark.sparkContext.textFile(stopwordsPath).collect())
        
        words = file.map(lambda line: line.split(",")).map(lambda parts: (parts[0][:4], [word for word in parts[1].split(" ") if word not in stopwords]))
        words = words.map(lambda line: (line[0], [word for word in line[1] if re.match(r'^[a-z]', word)]))
        pairs = words.flatMap(lambda pair: [((pair[0], combination)) for combination in itertools.combinations(pair[1], 2)])
 
        df = spark.createDataFrame(pairs, ["year", "words"])
        df = df.withColumn("words", col("words").cast("string"))
        df = df.withColumn("words", regexp_replace(df["words"], "[{}]", ""))
        df = df.withColumn("words", split(df["words"], ", "))
        '''
        sorting the words column so that it can treat (u,w) and (w,u) pair as the same
        '''
        df = df.withColumn("words", array_sort(df["words"]))
        df = df.withColumn("count", lit(1))
        '''
        df.show(truncate=False)
        create a df like this:
        +----+---------------------+-----+
        |year|words                |count|
        +----+---------------------+-----+
        |2003|[chief, council]     |1    |
        |2003|[council, executive] |1    |
        |2003|[council, fails]     |1    |
        |2003|[council, secure]    |1    |
        |2003|[council, position]  |1    |
        |2003|[chief, executive]   |1    |
        |2003|[chief, fails]       |1    |
        |2003|[chief, secure]      |1    |
        |2003|[chief, position]    |1    |
        |2003|[executive, fails]   |1    |
        |2003|[executive, secure]  |1    |
        |2003|[executive, position]|1    |
        |2003|[fails, secure]      |1    |
        |2003|[fails, position]    |1    |
        |2003|[position, secure]   |1    |
        |2003|[council, welcomes]  |1    |
        |2003|[ambulance, council] |1    |
        |2003|[council, levy]      |1    |
        |2003|[council, decision]  |1    |
        |2003|[ambulance, welcomes]|1    |
        +----+---------------------+-----+
        '''
        new_df = df.groupBy("year", "words").count().withColumnRenamed("count", "total_count")
        new_df = new_df.orderBy(new_df["year"].asc(), new_df["total_count"].desc(), new_df["words"].asc())
        '''
        new_df.show(truncate=False)
        after sorting the df:
        +----+-------------------------+-----------+
        |year|words                    |total_count|
        +----+-------------------------+-----------+
        |2003|[council, welcomes]      |2          |
        |2003|[ambulance, council]     |1          |
        |2003|[ambulance, decision]    |1          |
        |2003|[ambulance, levy]        |1          |
        |2003|[ambulance, welcomes]    |1          |
        |2003|[breakthrough, council]  |1          |
        |2003|[breakthrough, insurance]|1          |
        |2003|[breakthrough, welcomes] |1          |
        |2003|[chief, council]         |1          |
        |2003|[chief, executive]       |1          |
        |2003|[chief, fails]           |1          |
        |2003|[chief, position]        |1          |
        |2003|[chief, secure]          |1          |
        |2003|[council, decision]      |1          |
        |2003|[council, executive]     |1          |
        |2003|[council, fails]         |1          |
        |2003|[council, insurance]     |1          |
        |2003|[council, levy]          |1          |
        |2003|[council, position]      |1          |
        |2003|[council, secure]        |1          |
        +----+-------------------------+-----------+
        '''
        # Define the window
        window = Window.partitionBy(new_df['year']).orderBy(new_df['total_count'].desc())
        # Assign row numbers within the window
        new_df = new_df.withColumn('rn', row_number().over(window))
        # Filter for top k
        df_topk = new_df.filter(new_df.rn <= k)
        # drop the row number column
        df_topk = df_topk.drop('rn')
        df_topk = df_topk.withColumn("words", concat_ws(",", df["words"]))
        '''
        df_topk.show(truncate=False)
        Top k rows for each year when k = 3
        +----+-------------------+-----------+
        |year|words              |total_count|
        +----+-------------------+-----------+
        |2003|council,welcomes   |2          |
        |2003|ambulance,council  |1          |
        |2003|ambulance,decision |1          |
        |2004|cowboys,eels       |2          |
        |2004|bush,castro        |1          |
        |2004|bush,cuban         |1          |
        |2020|coronavirus,economy|2          |
        |2020|aid,china          |1          |
        |2020|aid,coronavirus    |1          |
        +----+-------------------+-----------+
        '''
        output_df = df_topk.withColumn("year", df_topk["year"].cast(StringType()))
        output_df = output_df.withColumn("words", regexp_replace(output_df["words"].cast(StringType()), "[{ }]", ""))
        output_df = output_df.withColumn("output", concat(col("year"), lit("    "), col("words"), lit(":"), col("total_count")))
        output_df = output_df.select("output")
        '''
        output_df.show(truncate=False)
        +-----------------------------+
        |output                       |
        +-----------------------------+
        |2003    council,welcomes:2   |
        |2003    ambulance,council:1  |
        |2003    ambulance,decision:1 |
        |2004    cowboys,eels:2       |
        |2004    bush,castro:1        |
        |2004    bush,cuban:1         |
        |2020    coronavirus,economy:2|
        |2020    aid,china:1          |
        |2020    aid,coronavirus:1    |
        +-----------------------------+
        '''
        output_df.coalesce(1).write.format("text").option("header", "true").save(outputPath)

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong inputs")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]))


