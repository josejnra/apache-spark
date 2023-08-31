## Apps availables on this docker-compose file
- Hadoop 3.3.6
- Apache Spark 3.4.1
- Apache Livy 0.7.1
- Jupyter Lab
- Elasticsearch 7.17.12
- Mysql 8.1.0
- Kafka 7.5.0
- Graphite 1.1.10
- Minio latest

You may simply change the environment variables `HADOOP_VERSION`, `SPARK_VERSION` and `LIVY_VERSION` on the [Dockerfile](Dockerfile) in order to create another docker image with different versions for the services. Besides that, you may have to update the [docker-compose.yaml](docker-compose.yaml) file to be according the new image.

### Build Image
```shell
docker-compose build
```

### Run
```shell
docker-compose up -d
```
Once you run this docker-compose, jupyter lab will be available at this [link](http://localhost:8888). If you want to use apache livy you must uncomment the command that starts the service on [docker-compose.yaml](docker-compose.yaml).

### Execute spark-submit
In order to run your apps, put all your app files within [apps](apps). Then just run:
```shell
./spark-submit.sh example.py
```


### Creates Spark Scala project

```shell
# spark project
sbt new holdenk/sparkProjectTemplate.g8
```
