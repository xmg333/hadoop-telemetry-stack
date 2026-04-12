#!/bin/bash
set -e
echo "Deploying Hadoop3 cluster..."
kubectl apply -f hadoop3/
echo "Deploying Hadoop2 cluster..."
kubectl apply -f hadoop2/
echo "Deploying Spark3..."
kubectl apply -f spark3/
echo "Deploying Spark2..."
kubectl apply -f spark2/
echo "Deploying Kafka..."
kubectl apply -f kafka/
echo "Deploying OTel Collector..."
kubectl apply -f otel-collector/
echo "Done! Use 'kubectl get pods' to check status."
