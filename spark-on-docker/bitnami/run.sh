#!/bin/bash

# Using Helm chart for spark from bitnami

kubectl cp example.py spark-release-worker-0:/tmp/

kubectl exec -it spark-release-worker-0 -- \
  spark-submit \
    --master "spark://spark-release-master-svc:7077" \
    /tmp/example.py
