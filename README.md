# Apache Spark
Apache Spark is a unified analytics engine for large-scale data processing. Spark applications run as independent sets of processes on a cluster, coordinated by the SparkContext object in your main program (called the driver program). SparkContext can connect to several types of cluster managers (either Spark’s own standalone cluster manager, Mesos or YARN), which allocate resources across applications.
<p align="center">
    <img src="images/spark-architecture.png" alt="Spark Architecture" />
</p>

## Concepts
### Application
User program built on Spark. Consists of a driver program and executors on the cluster. `spark-submit` is to submit a Spark application for execution (not Spark jobs). A single Spark application can have at least one Spark job.

### Driver Program
The process running the main() function of the application and creating the SparkContext. The driver does not run computations (filter, map, reduce, etc). When `collect()` is called on an RDD or Dataset, the whole data is sent to the Driver.

### Executor
A process launched for an application on a worker node, that runs tasks and keeps data in memory or disk storage across them. Each application has its own executors. So, executors are JVMs that run on Worker nodes. These are the JVMs that actually run **Tasks** on data **Partitions**.

### Cluster manager
An external service for acquiring resources on the cluster (e.g. standalone manager, Mesos, YARN).

### Job
A parallel computation consisting of multiple tasks that gets spawned in response to a Spark action (e.g. save, collect). A **Job** is a sequence of **Stages**, triggered by an **Action** such as `.count()`, `foreachRdd()`, `collect()`, `read()` or `write()`.
### Stage
Each job gets divided into smaller sets of tasks called stages that depend on each other (similar to the map and reduce stages in MapReduce). A Stage is a sequence of **Tasks** that can all be run together, in **parallel**, without a shuffle. For example: using .read to read a file from disk, then runnning .map and .filter can all be done without a shuffle, so it can fit in a single stage. The number of Tasks in a Stage also depends upon the number of Partitions your datasets have.

### Task
A unit of work that will be sent to one executor. A Task is a single operation (`.map` or `.filter`) applied to a single Partition. Each Task is executed as a single thread in an Executor. If your dataset has 2 **Partitions**, an operation such as a `filter()` will trigger 2 Tasks, one for each **Partition**.

### Shuffle
A Shuffle refers to an operation where data is re-partitioned across a Cluster. `join` and any operation that ends with `ByKey` will trigger a Shuffle. It is a costly operation because a lot of data can be sent via the network.

### Partition
A Partition is a logical chunk of your RDD/Dataset/DataFrame. Data is split into Partitions so that each Executor can operate on a single part, enabling parallelization. It can be processed by a single Executor core. For example: If you have 4 data partitions and you have 4 executor cores, you can process each Stage in parallel, in a single pass.

## Hadoop vs Spark
### Hadoop
- Slower performance, uses disks for storage and depends on disk read and write speed.
- Best for batch processing. Uses MapReduce to split a large dataset across a cluster for parallel analysis. 
- More difficult to use with less supported languages. Uses Java or Python for MapReduce apps.

### Spark
- Faster in-memory performance with reduced disk reading and writing operations. 
- Suitable for iterative and live-stream data analysis. Works with RDDs and DAGs to run operations.
- More user friendly. Allows interactive shell mode. APIs can be written in Java, Scala, R, Python, Spark SQL. 

## Run Spark Locally
At [spark-on-docker](spark-on-docker) you may run spark locally in several different ways. Using docker-compose or even on kubernetes.

## Jupyter Notebook
In the following link is shown two methods of how to use pyspark with jupyter notebook.
[https://www.sicara.ai/blog/2017-05-02-get-started-pyspark-jupyter-notebook-3-minutes](https://www.sicara.ai/blog/2017-05-02-get-started-pyspark-jupyter-notebook-3-minutes)

## Some Spark Parameters
Most of the values are the default ones.
```shell
spark-submit --master yarn \ # Default is None
             --deploy-mode client \ # Default is None. Options [cluster, client]
             --conf spark.executor.instances=1 \ # Dynamically defined.
             --conf spark.executor.cores=1 \ # 1 in YARN mode, all in standalone and mesos. Means that each executor can run a maximum of 1 task at the same time.
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
             --conf spark.checkpoint.compress=true \ # Compress RDD checkpoints. Default is false
             --conf spark.io.compression.codec=lz4 \ # Codec used to compress internal data such as RDD partitions, event log, broadcast variables and shuffle outputs. Options: lz4, lzf, snappy, zstd.
             --conf spark.task.cpus=1 \ # Number of cores to allocate for each task.
             --conf spark.task.maxFailures=4 \ # Number of failures of any particular task before giving up on the job.
             --conf spark.sql.avro.compression.codec=snappy \ # Compression codec used in writing of AVRO files. Supported codecs: uncompressed, deflate, snappy, bzip2 and xz.
             --conf spark.sql.parquet.compression.codec=snappy \ # Compression codec used when writing Parquet files. Acceptable values include: none, uncompressed, snappy, gzip, lzo, brotli, lz4, zstd.
             --conf spark.sql.shuffle.partitions=200 \ # The default number of partitions to use when shuffling data for joins or aggregations.
             --conf spark.sql.adaptive.enabled=true \ # Enable adaptive query execution, which re-optimizes the query plan in the middle of query execution, based on accurate runtime statistics. (Fixing skew) Default: false
             --conf spark.sql.sources.partitionOverwriteMode=static \ # Currently support 2 modes: static and dynamic. In static mode, Spark deletes all the partitions that match the partition specification in the INSERT statement, before overwriting. In dynamic mode, Spark doesn't delete partitions ahead, and only overwrite those partitions that have data written into it at runtime.
             --conf spark.yarn.maxAppAttempts=1 \ # Number of attempts on executing an application on YARN.
```

## Referencies
- [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html#yarn)
- [Spark's Hadoop Free](https://spark.apache.org/docs/latest/hadoop-provided.html)
- [Hadoop Single Node Cluster](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html)
- [Spark on Top of Hadoop YARN Cluster](https://www.linode.com/docs/guides/install-configure-run-spark-on-top-of-hadoop-yarn-cluster/)
- [Monitoring (Spark History)](https://spark.apache.org/docs/latest/monitoring.html)
- [Recommended Settings for Writing to Object Store](https://spark.apache.org/docs/3.2.0/cloud-integration.html#recommended-settings-for-writing-to-object-stores)
- [Some Concepts](https://queirozf.com/entries/apache-spark-architecture-overview-clusters-jobs-stages-tasks)
- [Hadoop vs Spark](https://phoenixnap.com/kb/hadoop-vs-spark)
- [Spark Tips. Partition Tuning](https://luminousmen.com/post/spark-tips-partition-tuning)
- [Bucketing in Pyspark](https://luminousmen.com/post/the-5-minute-guide-to-using-bucketing-in-pyspark)
- [Bucketing](https://towardsdatascience.com/best-practices-for-bucketing-in-spark-sql-ea9f23f7dd53)
- [Best Practices for Bucketing in Spark SQL](https://towardsdatascience.com/best-practices-for-bucketing-in-spark-sql-ea9f23f7dd53)
- [Structured Streaming Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
