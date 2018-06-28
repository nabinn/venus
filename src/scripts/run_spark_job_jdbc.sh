#!/bin/bash

# script for spark-submit

# takes in 4 arguments:
# 1. the name of python script inside spark folder of the project
# 2. transaction year
# 3. transaction month
# 4. transaction day

# Note:
# arguments 3 and 4 are optional i.e if they are not provided, spark 
# pulls out whole month or whole year worth of data from s3 bucket

# run this script to directly save spark dataframe to mysql table using jdbc

PYTHON_SCRIPT=$1
YEAR=$2
MONTH=$3
DAY=$4
$SPARK_HOME/bin/spark-submit \
--packages mysql:mysql-connector-java:5.1.40 \
--master spark://ip-10-0-0-11:7077 \
--executor-memory 6G \
/home/ubuntu/venmo/spark/$PYTHON_SCRIPT $YEAR $MONTH $DAY
