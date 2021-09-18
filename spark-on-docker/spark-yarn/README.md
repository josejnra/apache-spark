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
