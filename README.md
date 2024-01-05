# Advanced_DB_Project
## About
Project for Advanced Topics in Database Systems course of ECE ntua , academic year 2023-2024.
## Setup
1. Follow the lab guide to install Hadoop-Spark over Okeanos.  
2. Start the necessary services by executing the following commands:
``` 
    start_dfs.sh  
    start_yarn.sh  
    $SPARK_HOME/sbin/start-history-server.sh
```  
3. Create ~/scripts directory.
```
    mkdir ~/scripts
```
4. Add the scripts contained in the "Setup Scripts" folder in the ~/scripts directory, then execute them in the following order:
```
    ./get_datasets.sh
    ./format_datasets.sh
    ./import_data_to_hdfs.sh
```
4. Install pip, then install pyspark and geopy
```
    sudo apt install python3-pip
    pip install pyspark
    pip install geopy
```
5. Download geopy and its dependencies, format its content in a .zip file and store it in the ~/scripts directory in order to use it in the second version of query 4:
```
    python3 -m pip download geopy -d ~/scripts
    sudo apt install zip
    sudo apt install unzip
    unzip geopy-2.4.1-py3-none-any.whl
    unzip geographiclib-2.0-py3-none-any.whl
    zip -r query_4_modules.zip geopy geographiclib
```
6. Remove unnecessary files:
```
    rm -r geographiclib
    rm -r geographiclib-2.0.dist-info
    rm geographiclib-2.0-py3-none-any.whl 
    rm -r geopy
    rm -r geopy-2.4.1.dist-info
    rm geopy-2.4.1-py3-none-any.whl 
```
## Running Task Scripts
Copy the scripts in the ~/scripts directory.  
Then run the task scripts in the ~/scripts directory by executing the following command (replace "script_name" with the desired script's name):
```
    python3 script_name.py
```
For the query 3 script (task 5 version), you must specify the number of spark executors to be used:
```
    #example call, choose between 2, 3 and 4 Spark executors. 
    python3 query_3.py 2  
```