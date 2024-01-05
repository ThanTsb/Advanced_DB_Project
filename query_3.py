from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import desc, to_date, year, count, col, udf
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType
import time, sys

#
#   Get Dataframes
#

#set executors to 2,3 or 4
executors = sys.argv[1]
cores = "1" #we can increase vcores of each executor to increase parallelism

#Set up config, 4 spark executors
spark_conf = SparkConf()
spark_conf.set("spark.executor.instances", executors)
spark_conf.set("spark.executor.cores", cores)

#Create SparkSession
spark = SparkSession \
    .builder \
    .config(conf = spark_conf) \
    .appName(f"Query 3 (Dataframe API)({executors} executors)") \
    .getOrCreate()


#Define Schemas for each dataset
rev_geo_schema = StructType([
    StructField("LAT", DoubleType()),
    StructField("LON", DoubleType()),
    StructField("Zip Code", IntegerType()),
])

income_schema = StructType([
    StructField("Zip Code", IntegerType()),
    StructField("Community", StringType()),
    StructField("Estimated Median Income", StringType())
])

income_schema_2 = StructType([
    StructField("Zip Code", IntegerType()),
    StructField("Estimated Median Income", DoubleType())
])

#Create dataframe from datasets, use infer schema to automatically create schema were preferable
crime_df = spark.read.csv("hdfs://okeanos-master:54310/datasets/Crime_Data_from_2010_to_Present.csv", header=True, inferSchema=True)
rev_geo_df = spark.read.csv("hdfs://okeanos-master:54310/datasets/revgecoding.csv", header=True, schema=rev_geo_schema)
income_df = spark.read.csv("hdfs://okeanos-master:54310/datasets/LA_income_2015.csv", header=True, schema=income_schema)

#Convert 'DATE OCC' and 'Date Rptd' fields to date, since they are identified as string in initial schema
crime_df = crime_df.withColumn('DATE OCC', to_date('DATE OCC', "MM/dd/yyyy '12:00:00 AM'"))
crime_df = crime_df.withColumn('Date Rptd', to_date('Date Rptd', "MM/dd/yyyy '12:00:00 AM'"))
crime_df = crime_df.withColumn('Year', year("DATE OCC"))

#
#   Query 3, Dataframe API
#

#convert income to number so we can sort them properly later
def convert_income_to_num(income):
    new_income_1 = income.replace('$','')
    new_income_2 = new_income_1.replace(',','.')
    return float(new_income_2)

#Register udf
income_to_num = udf(convert_income_to_num, "double")

#start clock
start_time = time.time()


#Filtering/Adjusting the dataframes

#Select crimes occured in 2015, then remove victimless crimes(checking victim descent is enough).
crime_df_filtered = crime_df.filter(col('Year') == 2015) \
                            .filter(col("Vict Descent").isNotNull())
                           
#keep only one code for each pair of coordinates
rev_geo_df_filtered = rev_geo_df.dropDuplicates(['LAT','LON'])

#Perform Necessary joins in order to keep only zip codes found in crimes and associate them with incomes

#Line 1: join filtered crime dataframe with rev_geo dataframe on Coordinates
#Line 2: keep only Victim Descent and Zip Code columns
#Line 3: join results with income dataframe on Zip Code
joined_df = rev_geo_df.join(crime_df_filtered,['LAT','LON']) \
                 .select(crime_df_filtered['Vict Descent'], rev_geo_df['Zip Code']) \
                 .join(income_df, 'Zip Code') 

#Line 1: keep only zip codes and E.M.I.
#Line 2: keep only 1 zip code if multiple match a pair of coordinates
#Line 3: convert E.M.I to number in order to order results by income
#Line 4: order zip codes by E.M.I. (descending)
codes_by_income = joined_df.select('Zip Code','Estimated Median Income') \
                           .dropDuplicates(['Zip Code','Estimated Median Income']) \
                           .withColumn('Estimated Median Income',income_to_num(col('Estimated Median Income'))) \
                           .orderBy(desc('Estimated Median Income'))
     
#keep Zip Codes with 3 highest and 3 lowest E.M.I.
max3 = codes_by_income.head(3)
min3 = codes_by_income.tail(3)

#create dataframes containing them

max_zips = spark.createDataFrame(data = [max3[0],max3[1],max3[2]], schema = income_schema_2)
min_zips = spark.createDataFrame(data = [min3[0],min3[1],min3[2]], schema = income_schema_2)

#keep crimes related only to 3 max zip codes
query_3_result_1 = joined_df.join(max_zips,'Zip Code') \
                          .groupby('Vict Descent').agg(count('*').alias('#Victims')) \
                          .orderBy(desc('#Victims'))

#show results
print("3 Zip Codes with Maximum E.M.I.")
print(query_3_result_1.show())

#keep crimes related only to 3 min zip codes
query_3_result_2 = joined_df.join(min_zips,'Zip Code') \
                          .groupby('Vict Descent').agg(count('*').alias('#Victims')) \
                          .orderBy(desc('#Victims'))

#show results
print("3 Zip Codes with Minimum E.M.I.")
print(query_3_result_2.show())
print(f"Time taken for 3rd query (Dataframe API) ({executors} Spark executors): {(time.time() - start_time)} seconds.")