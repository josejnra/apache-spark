#!/bin/bash

# k8s as cluster manager
spark-submit \
            --master k8s://https://192.168.49.2:8443 \
            --deploy-mode cluster \
            --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
            --conf spark.executor.instances=1 \
            --conf spark.kubernetes.driver.pod.name=spark-driver \
            --conf spark.kubernetes.container.image=myspark:latest \
            local:///example.py
