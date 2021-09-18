## Run Spark
At [spark-on-docker](spark-on-docker) you may run spark locally in several different ways. Using docker-compose or even on kubernetes.

## Linux Configuration
Set up the environment by defining the environment variables in your `~/.bashrc`.
```shell
# easily define which version of spark and python to use 
export PYTHON_FOR_SPARK=~/venvs/python37/bin
export SPARK_2_4_7=~/Frameworks/spark-2.4.7-bin-hadoop2.7
export SPARK_3_0_2=~/Frameworks/spark-3.0.2-bin-hadoop2.7

###############
# SPARK CONFIG
###############
export SPARK_HOME=$SPARK_3_0_2
export PATH=$PATH:$SPARK_HOME/bin
export PYSPARK_PYTHON=$PYTHON_FOR_SPARK/python

# run pyspark with ipython
export PYSPARK_DRIVER_PYTHON=$PYTHON_FOR_SPARK/ipython

```

## Jupyter Notebook
In the following link is shown two methods of how to use pyspark with jupyter notebook.
[https://www.sicara.ai/blog/2017-05-02-get-started-pyspark-jupyter-notebook-3-minutes](https://www.sicara.ai/blog/2017-05-02-get-started-pyspark-jupyter-notebook-3-minutes)


## Spark Submit Using Docker Images
We may use docker images from [Data Mechanics](https://hub.docker.com/r/datamechanics/spark) in order
to run spark apps without the need of installing and configuring it.
On this [folder](spark-on-docker) all you need to do is to map the source code volume into `/opt/application` and 
change the `command` on the docker-compose file. Then, just `docker-compose up`.


## Some Spark Parameters
Most of the values are the default ones.
```shell
spark-submit --master yarn \ # Default is None
             --deploy-mode client \ # Default is None. Options [cluster, client]
             --conf spark.executor.instances=2 \ 
             --conf spark.executor.cores=3 \ # Cores per executor.
             --conf spark.executor.memory=1G \ # Memory per executor.
             --conf spark.executor.memoryOverhead=384M \ # Amount of additional memory to be allocated per executor process. Default: executorMemory * 0.10, with minimum of 384.
             --conf spark.executor.heartbeatInterval=10s \ # Interval between each executor's heartbeats to the driver. should be significantly less than spark.network.timeout.
             --conf spark.driver.cores=1 \ # Only in cluster mode.
             --conf spark.driver.memory=1G \ # where SparkContext is initialized.
             --conf spark.driver.maxResultSize=1G \ # Total size of results of all partitions for each Spark action. At least 1M, 0 for unlimited.
             --conf spark.driver.memoryOverhead=1G \ # Non-heap memory to be allocated per driver process in cluster mode. Default: driverMemory * 0.10, with minimum of 384.
             --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \ # Use Kryo, the default is quite slow.
             --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \ # 1 is the default. This does less renaming at the end of a job. v2 algorithm for performance; v1 for safety.
             --conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored=true \ # Ignore failures when cleaning up temporary files
             --conf spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs=false \ # Avoid _SUCCESS files being generated. Default is true.
             --conf spark.hadoop.parquet.enable.summary-metadata=false \ # Parquet: minimise the amount of data read during queries.
             --conf spark.sql.parquet.mergeSchema=false \ # Parquet: minimise the amount of data read during queries.
             --conf spark.sql.parquet.filterPushdown=true \ # Parquet: minimise the amount of data read during queries.
             --conf spark.sql.hive.metastorePartitionPruning=true \ # Parquet: minimise the amount of data read during queries.
             --conf spark.jars=https://path/to/my/file.jar \ # Comma-separated list of jars to include on the driver and executor classpaths. Supports following URL scheme hdfs/http/https/ftp.
             --conf spark.jars.packages=groupId:artifactId:version # Comma-separated list of Maven coordinates of jars to include on the driver and executor classpaths.
```

## Referencies
- [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html#yarn)
- [Spark's Hadoop Free](https://spark.apache.org/docs/latest/hadoop-provided.html)
- [Hadoop Single Node Cluster](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html)
- [Spark on Top of Hadoop YARN Cluster](https://www.linode.com/docs/guides/install-configure-run-spark-on-top-of-hadoop-yarn-cluster/)
- [Monitoring (Spark History)](https://spark.apache.org/docs/latest/monitoring.html)
- [Recommended Settings for Writing to Object Store](https://spark.apache.org/docs/3.1.2/cloud-integration.html#recommended-settings-for-writing-to-object-stores)
