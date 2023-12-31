from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import to_date


#Set up config, 4 spark executors
spark_conf = SparkConf()
spark_conf.set("spark.executor.instances", "4")
spark_conf.set("spark.executor.cores","2")

#Create SparkSession
spark = SparkSession \
    .builder \
    .config(conf = spark_conf) \
    .appName("Parse LA Crime Data (Task 2)") \
    .getOrCreate()

#Create dataframe from dataset, use infer schema to automatically create schema
crime_df = spark.read.csv("hdfs://okeanos-master:54310/datasets/Crime_Data_from_2010_to_Present.csv", header=True, inferSchema=True)

#Convert 'DATE OCC' and 'Date Rptd' fields to date, since they are identified as string in initial schema
crime_df = crime_df.withColumn('DATE OCC', to_date('DATE OCC', "MM/dd/yyyy '12:00:00 AM'"))
crime_df = crime_df.withColumn('Date Rptd', to_date('Date Rptd', "MM/dd/yyyy '12:00:00 AM'"))

#Print schema
print("Schema of LA Crime Data:")
print(crime_df.printSchema())

#Count number of rows
rows = crime_df.count()

# Print total number of rows
print(f"Total number of rows for LA Crime Data: {rows}")