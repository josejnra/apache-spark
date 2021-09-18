## How to Run
### Build Image
```shell
docker-compose build
```

### Run Spark, Hadoop and YARN
```shell
docker-compose up -d
```

### Run Apps
In order to run your apps, put all your app files within [apps](apps). Edit the [spark-submit.sh](spark-submit.sh) to point to your app, then just run:
```shell
./spark-submit.sh
```

## Spark and Hadoop Versions
You may just change the environment variables `HADOOP_VERSION` and `SPARK_VERSION` on the [Dockerfile](Dockerfile) in order to create another docker image with 
different versions of spark and hadoop. Besides that, you have to update the [docker-compose.yaml](docker-compose.yaml) file to be according the new image.
