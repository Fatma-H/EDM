#!/bin/bash
main_Class=org.example.Main
app_jar=/root/EDM/target/Try-1.0-SNAPSHOT.jar
master_url="spark://fatmah:7077"

# Submit to spark
./spark-submit \
    --class $main_Class \
    --conf spark.driver.memory=1g \
    --master $master_url \
    $app_jar
    
# Build Spark-Hadoop Docker image
docker build -f docker/Dockerfile -t spark-hadoop:3.3.1 ./docker

# Deploy master and worker on kubernets 
kubectl create -f ./kubernetes/spark-master-deployment.yaml
kubectl create -f ./kubernetes/spark-master-service.yaml
kubectl create -f ./kubernetes/spark-worker-deployment.yaml

# Submit job to kubernets
spark-submit \
    --master k8s://https://kubernetes.docker.internal:6443 \
    --deploy-mode cluster \
    --class $main_Class \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=newfrontdocker/spark:v3.0.1-j14 \
    $app_jar
