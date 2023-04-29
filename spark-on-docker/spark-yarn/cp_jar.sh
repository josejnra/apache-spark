#!/bin/bash

docker cp ./apps/scala-project/target/scala-2.12/scala-project_2.12-0.1.0-SNAPSHOT.jar  spark:/opt/

# docker cp ./spark-hive_2.12-3.4.0.jar  spark:/opt/
