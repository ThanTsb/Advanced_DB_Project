from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import avg, asc, desc, to_date, year, count, col, udf, min
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType
import os, sys, time

from geopy.distance import geodesic

#
#   Get Dataframes
#


executors = "4"
cores = "2"

#Set up config, 4 spark executors, add geopy and its dependencies to executor and driver python path
spark_conf = SparkConf()
spark_conf.set("spark.executor.instances", executors)
spark_conf.set("spark.executor.cores", cores)
spark_conf.set("spark.submit.pyFiles",os.path.join(os.getcwd(), "query_4_modules.zip"))

#Create SparkSession
spark = SparkSession \
    .builder \
    .config(conf = spark_conf) \
    .appName(f"Query 4 (Dataframe API) (Second Version))") \
    .getOrCreate()

#set up lapd schema
lapd_schema = StructType([
    StructField("LAPD LON", DoubleType()),
    StructField("LAPD LAT", DoubleType()),
    StructField("FID", IntegerType()),
    StructField("DIVISION", StringType()),
    StructField("LOCATION", StringType()),
    StructField("AREA", IntegerType())
])

#Create dataframe from dataset, use infer schema to automatically create schema
crime_df = spark.read.csv("hdfs://okeanos-master:54310/datasets/Crime_Data_from_2010_to_Present.csv", header=True, inferSchema=True)
lapd_df = spark.read.csv("hdfs://okeanos-master:54310/datasets/LAPD_Police_Stations.csv", header=True, schema=lapd_schema)

#Convert 'DATE OCC' and 'Date Rptd' fields to date, since they are identified as string in initial schema
crime_df = crime_df.withColumn('DATE OCC', to_date('DATE OCC', "MM/dd/yyyy '12:00:00 AM'"))
crime_df = crime_df.withColumn('Date Rptd', to_date('Date Rptd', "MM/dd/yyyy '12:00:00 AM'"))

#Dataframe API

#
#   query 4a (second version)
#

# calculate the distance between two points [lat1,long1], [lat2, long2] in km
def get_distance(lat1,long1,lat2,long2):
    return geodesic((lat1, long1), (lat2, long2)).km

#register udf
udf_distance = udf((get_distance), "double")

#start clock
start_time = time.time()

#filter (0,0) entries, get only firearm crimes, seperate year from date occured
crime_df_filtered = crime_df.filter(col("LAT") != 0) \
                            .filter(col("Weapon Used Cd").startswith('1')) \
                            .withColumn('YEAR', year(col('DATE OCC')))

#crossJoin dataframes, then find the pd with minimum distance for each crime
minimum_distances_df = crime_df_filtered.crossJoin(lapd_df) \
                                        .withColumn('DISTANCE', udf_distance(col('LAT'), col('LON'), col('LAPD LAT'), col('LAPD LON'))) \
                                        .groupBy('DR_NO') \
                                        .agg(min('DISTANCE').alias('MIN_DISTANCE'))

#join with the filtered crime dataframe to get other needed columns 
new_crime_df = crime_df_filtered.join(minimum_distances_df, "DR_NO")

#group by year, then get average distance for each year
df_average_distances = new_crime_df.groupBy('YEAR') \
                                   .agg(avg('MIN_DISTANCE').alias('AVG_DISTANCE')) 

#group by year, then get # of crimes for each year
df_num_crimes = new_crime_df.groupBy('YEAR') \
                            .agg(count('*').alias('NUM_CRIMES')) \
                            
#join the 2 previous dataframes                            
query_4a_2 = df_average_distances.join(df_num_crimes,'YEAR') \
                                 .orderBy(asc('YEAR'))

#show results of query 4a (second version)

output_4a_2 = query_4a_2.show(14)

print(f"Time taken for query 4_a (Dataframe API) (Second Version) ): {(time.time() - start_time)} seconds.")

query_4a_2.explain(mode="formatted")

print(output_4a_2)

#
#   query 4b (second version)
#

#start clock
start_time = time.time()

#filter (0,0) entries, get crimes with weapons, seperate year from date occured
crime_df_filtered_2 = crime_df.filter(col("LAT") != 0) \
                              .filter(col("Weapon Used Cd").isNotNull()) \
                              .withColumn('YEAR', year(col('DATE OCC')))

#crossJoin dataframes, then calculate distances from each possible pd for each crime
distances_df_2 = crime_df_filtered_2.crossJoin(lapd_df) \
                                    .withColumn('DISTANCE', udf_distance(col('LAT'), col('LON'), col('LAPD LAT'), col('LAPD LON'))) 

#keep only minimum for each crime on a separate df, alias minimun distance as distance to join later
minimum_distances_df_2 = distances_df_2.groupBy('DR_NO') \
                                       .agg(min('DISTANCE').alias('DISTANCE'))

#join the temp df with distances_df_2 on DR_NO and DISTANCE, in order to get pd names with minimum distance 
new_crime_df_2 = distances_df_2.join(minimum_distances_df_2, ['DR_NO','DISTANCE'], how = "right")
                                
new_crime_df_3 = new_crime_df_2.filter(col('DISTANCE').isNotNull())

#group by division, then get average distance for each division
df_average_distances_2 = new_crime_df_3.groupBy('DIVISION') \
                                       .agg(avg('DISTANCE').alias('AVG_DISTANCE')) 

#group by division, then get # of crimes for each division
df_num_crimes_2 = new_crime_df_3.groupBy('DIVISION') \
                                .agg(count('*').alias('NUM_CRIMES')) 
                            
#join the 2 previous dataframes                            
query_4b_2 = df_average_distances_2.join(df_num_crimes_2,'DIVISION') \
                                 .orderBy(desc('NUM_CRIMES'))

#show results of query 4b (second version)
output_4b_2 = query_4b_2.show(22)

print(f"Time taken for query 4_b (Dataframe API) (Second Version)): {(time.time() - start_time)} seconds.")

query_4b_2.explain(mode="formatted")

print(output_4b_2)