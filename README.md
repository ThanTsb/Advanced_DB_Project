# Advanced_DB_Project
## About
Project for Advanced Topics in Database Systems course of ECE ntua , academic year 2023-2024.
## Setup
1. Follow the lab guide to install Hadoop-Spark over Okeanos.  
2. Start the necessary services by executing the following commands:
``` 
    start_dfs.sh  
    start_yarn.sh  
    $SPARK_HOME/sbin/stop-history-server.sh
```  
3. Create ~/scripts directory.
4. Add the scripts contained in the "Setup Scripts" folder in the~/scripts directory, then execute them in the following order:
```
    ./get_datasets.sh
    ./format_datasets.sh
    ./import_data_to_hdfs.sh
```
4. Install pip, then install pyspark
5. Download geopy and its dependencies, format its content in a .zip file and store it in the ~/scripts directory.
## Running Task Scripts
Copy the scripts in the ~/scripts directory.  
Then run the task scripts in the ~/scripts directory by executing the following command (replace "script_name" with the desired script's name):
```
    python3 script_name.py
```
For the query 3 scripts, specify Join Strategy and number of Spark Executors (in that order) as input parameters:
```
  #example call
  python3 query_3.py broadcast 3
```
For the query 4 scripts, specify Join Strategy as input parameter:
```
  #example call
  python3 query_3.py broadcast
```