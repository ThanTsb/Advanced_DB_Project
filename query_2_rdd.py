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

#Create SparkSession, include sparkContext since we will be using RDD API
spark = SparkSession \
    .builder \
    .config(conf = spark_conf) \
    .appName("Query 2 (RDD API)") \
    .getOrCreate() 
    
#Create dataframe from dataset, use infer schema to automatically create schema
crime_df = spark.read.csv("hdfs://okeanos-master:54310/datasets/Crime_Data_from_2010_to_Present.csv", header=True, inferSchema=True)

#Convert 'DATE OCC' and 'Date Rptd' fields to date, since they are identified as string in initial schema
crime_df = crime_df.withColumn('DATE OCC', to_date('DATE OCC', "MM/dd/yyyy '12:00:00 AM'"))
crime_df = crime_df.withColumn('Date Rptd', to_date('Date Rptd', "MM/dd/yyyy '12:00:00 AM'"))

#
#   Query 2, RDD API
#

#get rdd from crime dataframe
crime_rdd = crime_df.rdd

#if we read the csv using textFile we parse some rows of the dataset incorrectly, 
#so we convert from dataframe instead

#crime_rdd = spark.textFile("hdfs://okeanos-master:54310/datasets/Crime_Data_from_2010_to_Present.csv") \
#                      .map(lambda x: x.split(","))

#udf func for time_of_day conversion
def get_time_of_day(time):
    int_time = int(time)
    if int_time >= 500 and int_time < 1160:
        return "morning"
    elif int_time >= 1200 and int_time < 1660:
        return "afternoon"
    elif int_time >= 1700 and int_time < 2060:
        return "evening"
    elif int_time >= 2100 and int_time < 2360:
        return "night"
    else:
        return "night"

#start clock
start_time = time.time()

#filter street crimes only, calculate time of day, then reduce by time of day (sort by descending # of crimes)
filtered_crime_rdd = crime_rdd.filter(lambda x: x[15] == "STREET") \
                              .map(lambda x: (get_time_of_day(x[3]), 1)) \
                              .reduceByKey(lambda x,y: x + y) \
                              .sortBy(lambda x: x[1], ascending=False)

#show results
print(filtered_crime_rdd.collect()) 

#show time taken
print(f"Time taken for 2nd query (RDD API): {(time.time() - start_time)} seconds.")
