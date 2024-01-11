from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import to_date
import time

#
#   Get dataframe
#

#Set up config, 4 spark executors
spark_conf = SparkConf()
spark_conf.set("spark.executor.instances", "4")
spark_conf.set("spark.executor.cores","2")

#Create SparkSession
spark = SparkSession \
    .builder \
    .config(conf = spark_conf) \
    .appName("Query 1 (SQL API)") \
    .getOrCreate()

#Create dataframe from dataset, use infer schema to automatically create schema
crime_df = spark.read.csv("hdfs://okeanos-master:54310/datasets/Crime_Data_from_2010_to_Present.csv", header=True, inferSchema=True)

#Convert 'DATE OCC' and 'Date Rptd' fields to date, since they are identified as string in initial schema
crime_df = crime_df.withColumn('DATE OCC', to_date('DATE OCC', "MM/dd/yyyy '12:00:00 AM'"))
crime_df = crime_df.withColumn('Date Rptd', to_date('Date Rptd', "MM/dd/yyyy '12:00:00 AM'"))

#
#   Query 1, SQL API
#

#Utilize crime_df as SQL table
crime_df.createOrReplaceTempView("crimes")

query_1 = '''
            SELECT subq.year AS year, subq.month AS month, subq.crime_total AS crime_total, subq.month_rank AS month_rank
            FROM (
                  SELECT  
                    YEAR(crimes.`DATE OCC`) AS year,
                    MONTH(crimes.`DATE OCC`) AS month,
                    COUNT(*) AS crime_total,
                    ROW_NUMBER() OVER (PARTITION BY YEAR(crimes.`DATE OCC`) ORDER BY COUNT(*) DESC) AS month_rank
                  FROM 
                    crimes
                  GROUP BY
                    YEAR(crimes.`DATE OCC`),
                    MONTH(crimes.`DATE OCC`)
                  ORDER BY
                    year ASC,
                    crime_total DESC
                 ) subq
            WHERE subq.month_rank < 4
        '''

#start clock
start_time = time.time()

#Execute query
results = spark.sql(query_1)


results.show(42)

#14 years , so show 3 * 14 = 42 results 
print(f"Time taken for 1st query (SQL API): {(time.time() - start_time)} seconds.")