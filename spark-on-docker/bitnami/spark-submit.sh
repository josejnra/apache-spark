#!/bin/bash

# delete app folder, if exists
docker container exec -it -u root spark-master bash -c "rm -rf /app"

# create dir for the app
docker container exec -it -u root spark-master bash -c "mkdir /app"

# copy app to container
docker cp example.py spark-master:/app/ 

COMMAND='spark-submit --master spark://spark:7077 \
                      /app/example.py'

docker container exec -it spark-master bash -c "${COMMAND}"
