from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf, desc, to_date, count, col
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
    .appName("Query 2 (Dataframe API)") \
    .getOrCreate()

#Create dataframe from dataset, use infer schema to automatically create schema
crime_df = spark.read.csv("hdfs://okeanos-master:54310/datasets/Crime_Data_from_2010_to_Present.csv", header=True, inferSchema=True)

#Convert 'DATE OCC' and 'Date Rptd' fields to date, since they are identified as string in initial schema
crime_df = crime_df.withColumn('DATE OCC', to_date('DATE OCC', "MM/dd/yyyy '12:00:00 AM'"))
crime_df = crime_df.withColumn('Date Rptd', to_date('Date Rptd', "MM/dd/yyyy '12:00:00 AM'"))

#
#   Query 2, Dataframe API
#

def get_time_of_day(time):
    if time >= 500 and time < 1160:
        return "morning"
    elif time >= 1200 and time < 1660:
        return "afternoon"
    elif time >= 1700 and time < 2060:
        return "evening"
    elif time >= 2100 and time < 2360:
        return "night"
    else:
        return "night"

#Register udf
time_of_day = udf(get_time_of_day, "string")

#start clock
start_time = time.time()

#Line 1: convert 'TIME OCC' column to IntegerType,
#Line 2: get time of day for each crime using udf time_of_day
#Line 3: keep only street crimes
#Line 4: group by time of day, count # of crimes for each period
#Line 5: order by # of crimes (descending) 
crime_df_query_2 = crime_df.withColumn('TIME OCC', crime_df['TIME OCC'].cast(IntegerType())) \
                   .withColumn("time_of_day", time_of_day(col("TIME OCC"))) \
                   .filter(col('Premis Desc') == 'STREET') \
                   .groupby("time_of_day").agg(count('*').alias('#crimes')) \
                   .orderBy(desc("#crimes"))
 
#Print results 
crime_df_query_2.show()

#Print time taken
print(f"Time taken for 2nd query (Dataframe API): {(time.time() - start_time)} seconds.")