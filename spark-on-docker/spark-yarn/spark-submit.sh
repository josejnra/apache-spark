#!/bin/bash

docker container exec -it spark bash -c "source ~/.bashrc && spark-submit /opt/apps/example.py"
