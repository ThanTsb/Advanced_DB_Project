from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.conf import SparkConf
from pyspark.sql.functions import asc, desc, to_date, year, month, count, col, row_number

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
    .appName("Query 1 (Dataframe API)") \
    .getOrCreate()

#Create dataframe from dataset, use infer schema to automatically create schema
crime_df = spark.read.csv("hdfs://okeanos-master:54310/datasets/Crime_Data_from_2010_to_Present.csv", header=True, inferSchema=True)

#Convert 'DATE OCC' and 'Date Rptd' fields to date, since they are identified as string in initial schema
crime_df = crime_df.withColumn('DATE OCC', to_date('DATE OCC', "MM/dd/yyyy '12:00:00 AM'"))
crime_df = crime_df.withColumn('Date Rptd', to_date('Date Rptd', "MM/dd/yyyy '12:00:00 AM'"))

#
#   Query 1, Dataframe API
#

#window used in line 4 for to rank months for each year
w = Window.partitionBy('year').orderBy(desc('crime_total'))

#Lines 1-2 : get year, month columns 
#Line 3: group by year and month and get # of crimes for each pair
#Line 4: get ranking of months for each year
#Line 5: keep only the best 3 months for each year
#Line 6: sort by year (ascending) and # of crimes (descending)
crime_df_query_1 = crime_df.withColumn('year', year("DATE OCC")) \
                           .withColumn('month', month("DATE OCC")) \
                           .groupby(["year", "month"]).agg(count('*').alias('crime_total')) \
                           .withColumn('month_rank', row_number().over(w)) \
                           .filter(col('month_rank') < 4) \
                           .orderBy(asc('year'),desc('crime_total'))

#print results
crime_df_query_1.show(42)
