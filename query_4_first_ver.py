from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import avg, asc, desc, to_date, year, count, col, udf
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType
import os, sys

from geopy.distance import geodesic

#
#   Get Dataframes
#

join_strat = sys.argv[1]
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
    .appName(f"Query 4 (Dataframe API) (First version) ({join_strat})") \
    .getOrCreate()

lapd_schema = StructType([
    StructField("LAPD LON", DoubleType()),
    StructField("LAPD LAT", DoubleType()),
    StructField("FID", IntegerType()),
    StructField("DIVISION", StringType()),
    StructField("LOCATION", StringType()),
    StructField("AREA ", IntegerType())
])

#Create dataframe from dataset, use infer schema to automatically create schema
crime_df = spark.read.csv("hdfs://okeanos-master:54310/datasets/Crime_Data_from_2010_to_Present.csv", header=True, inferSchema=True)
lapd_df = spark.read.csv("hdfs://okeanos-master:54310/datasets/LAPD_Police_Stations.csv", header=True, schema=lapd_schema)

#Convert 'DATE OCC' and 'Date Rptd' fields to date, since they are identified as string in initial schema
crime_df = crime_df.withColumn('DATE OCC', to_date('DATE OCC', "MM/dd/yyyy '12:00:00 AM'"))
crime_df = crime_df.withColumn('Date Rptd', to_date('Date Rptd', "MM/dd/yyyy '12:00:00 AM'"))

#Dataframe API

#
#   query 4a (first version)
#

# calculate the distance between two points [lat1,long1], [lat2, long2] in km
def get_distance(lat1,long1,lat2,long2):
    return geodesic((lat1, long1), (lat2, long2)).km

#register udf
udf_distance = udf((get_distance), "double")

#filter (0,0) entries, get only firearm crimes, seperate year from date occured
crime_df_filtered = crime_df.filter(col("LAT") != 0) \
                            .filter(col("Weapon Used Cd").startswith('1')) \
                            .withColumn('YEAR', year(col('DATE OCC')))

#join dataframes,calculate distance from pds to area committed for each crime
joined_df = crime_df_filtered.join(lapd_df.hint(join_strat),'AREA ') \
                             .withColumn('DISTANCE', udf_distance(col('LAT'), col('LON'), col('LAPD LAT'), col('LAPD LON'))) 

#get average distance for each year
df_average_dist = joined_df.groupBy('YEAR') \
                           .agg(avg(col('DISTANCE')).alias('AVG_DISTANCE')) 

#get number of crimes for each year
df_num_crimes = joined_df.groupBy('YEAR') \
                         .agg(count('*').alias('NUM_CRIMES')) 

#join the 2 previous dataframes on year, sort by year (ascending)
query_4a_1 = df_average_dist.join(df_num_crimes.hint(join_strat), 'YEAR') \
                            .orderBy(asc('YEAR'))

#show results of query 4a (first version)
query_4a_1.explain(mode="formatted")

output_4a_1 = query_4a_1.show(14)
print(output_4a_1)

#
#   query 4b (first version)
#


#filter (0,0) entries, get crimes with weapons, seperate year from date occured
crime_df_filtered_2 = crime_df.filter(col("LAT") != 0) \
                              .filter(col("Weapon Used Cd").isNotNull()) \
                              .withColumn('YEAR', year(col('DATE OCC')))

#join dataframes and calculate distances from pds
joined_df_2 = crime_df_filtered_2.join(lapd_df.hint(join_strat),'AREA ') \
                               .withColumn('DISTANCE', udf_distance(col('LAT'), col('LON'), col('LAPD LAT'), col('LAPD LON'))) 

#group by division, calculate average distance from pds to area committed, then sort by # of crimes
joined_df_distances = joined_df_2.groupBy('DIVISION') \
                                 .agg(avg('DISTANCE').alias('AVG_DISTANCE'))


joined_df_num_crimes = joined_df_2.groupBy('DIVISION') \
                                  .agg(count('*').alias('NUM_CRIMES')) \

#join the 2 previous dataframes on Division, sort by # of crimes (descending)
query_4b_1 = joined_df_distances.join(joined_df_num_crimes.hint(join_strat), 'DIVISION') \
                            .orderBy(desc('NUM_CRIMES'))

#show results of query 4b (first version)
query_4b_1.explain(mode="formatted")

output_4b_1 = query_4b_1.show(22)
print(output_4b_1)