## Apps availables on this Docker Image
- Hadoop 3.3.1
- Apache Spark 3.1.2
- Apache Livy 0.7.1
- Jupyter Lab

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
In order to run your apps, put all your app files within [apps](apps). Edit the [spark-submit.sh](spark-submit.sh) to point to your app, then just run:
```shell
./spark-submit.sh
```
