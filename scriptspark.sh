#!/bin/bash

# Define the Spark configuration
master_url="spark://fatmah:7077"
deploy_mode="client"
executor_cores=1
executor_memory="1g"

# Define the application JAR file
app_jar="/root/EDM/target/Try-1.0-SNAPSHOT.jar"
main_class="org.example.Main"

# Define the input and output directories
#input_dir="/root/spark-3.3.1-bin-hadoop3-scala2.13/src/test/resources/data/input/liste-acv-com2022-20221118.csv"
input_dir="/root/EDM/src/test/resources/data/input/liste-acv-com2022-20221118.csv"

# Define environment variables
env_vars="-Dinput=$input_dir"

# Execute the spark-submit command
./bin/spark-submit --master $master_url \
                  --deploy-mode $deploy_mode \
                  --executor-cores $executor_cores \
                  --executor-memory $executor_memory \
                  --class $main_class \
                  --files $input_dir \
                  --conf spark.driver.extraJavaOptions=$env_vars \
                  --conf spark.executor.extraJavaOptions=$env_vars \
                  $app_jar
