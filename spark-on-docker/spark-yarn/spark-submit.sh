#!/bin/bash

SCRIPT_PATH=$1

docker container exec -it spark bash -c "source ~/.bashrc && spark-submit /opt/apps/${SCRIPT_PATH}"
