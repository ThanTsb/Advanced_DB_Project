#Create directory in HDFS to store the datasets

hdfs dfs -mkdir hdfs://okeanos-master:54310/datasets

#Upload the CSV files in the hdfs
hdfs dfs -put ~/datasets/Crime_Data_from_2010_to_Present.csv hdfs://okeanos-master:54310/datasets/Crime_Data_from_2010_to_Present.csv
hdfs dfs -put ~/datasets/revgecoding.csv hdfs://okeanos-master:54310/datasets/revgecoding.csv
hdfs dfs -put ~/datasets/income/LA_income_2015.csv hdfs://okeanos-master:54310/datasets/LA_income_2015.csv
hdfs dfs -put ~/datasets/LAPD_Police_Stations.csv hdfs://okeanos-master:54310/datasets/LAPD_Police_Stations.csv