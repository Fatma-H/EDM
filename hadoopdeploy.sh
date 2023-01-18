#!/bin/bash

# Set the path to the jar file
JAR_FILE=/home/haadoop/hadoop-3.3.2/EDM/target/Try-1.0-SNAPSHOT.jar

# Set the main class of the application
MAIN_CLASS=org.example.Main


# Set the YARN queue
QUEUE=default

# Set the number of containers to use
NUM_CONTAINERS=1

# Set the amount of memory to use for each container
CONTAINER_MEM=1024

# Set the amount of virtual cores to use for each container
CONTAINER_VCORES=1

# Submit the application to YARN
yarn jar $JAR_FILE $MAIN_CLASS -Dmapreduce.job.queuename=$QUEUE  -Dmapreduce.map.memory.mb=$CONTAINER_MEM -Dmapreduce.map.cpu.vcores=$CONTAINER_VCORES -Dmapreduce.job.ubertask.enable=false -Dmapreduce.job.reduces=$NUM_CONTAINERS 
