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

### Spark Context
Spark 1.x comes with three entry points: SparkContext, SQLContext, and HiveContext. And with the introduction of Spark 2.x, a new entry point named SparkSession was added. As a result, this single entry point effectively combines all of the functionality available in the three aforementioned contexts.

SparkContext is the primary point of entry for Spark capabilities. A SparkContext represents a Spark cluster’s connection that is useful in building RDDs, accumulators, and broadcast variables on the cluster. It enables your Spark Application to connect to the Spark Cluster using Resource Manager. Also, before the creation of SparkContext, SparkConf must be created. __After creating the SparkContext, you can use it to create RDDs, broadcast variables, and accumulators, as well as access Spark services and perform jobs__. All of this can be done until SparkContext is terminated. Access to the other two contexts, SQLContext and HiveContext, is also possible through SparkContext. Since Spark 2.0, most SparkContext functions are also available in SparkSession. SparkContext’s default object sc is provided in Spark-Shell, and it can also be constructed programmatically using the SparkContext class. As a result, SparkContext provides numerous Spark functions. This includes getting the current status of the Spark Application, setting the configuration, canceling a task, canceling a stage, and more. It was a means to get started with all the Spark features prior to the introduction of SparkSession.
### Spark Session
Previously, as RDD was the major API, SparkContext was the entry point for Spark. It was constructed and modified with the help of context APIs. At that time, we have to use a distinct context for each API. We required StreamingContext for Streaming, SQLContext for SQL, and HiveContext for Hive. However, because the DataSet and DataFrame APIs are becoming new independent APIs, we require an entry-point construct for them. As a result, in Spark 2.0, we have a new __entry point built for DataSet and DataFrame APIs called SparkSession__.

It combines SQLContext, HiveContext, and StreamingContext. All of the APIs accessible in those contexts are likewise available in SparkSession, and SparkSession includes a SparkContext for real computation. It’s worth noting that the previous SQLContext and HiveContext are still present in updated versions, but only for backward compatibility. As a result, when comparing SparkSession vs SparkContext, as of Spark 2.0.0, it is better to use SparkSession because it provides access to all of the Spark features that the other three APIs do. Its Spark object comes by default in Spark-shell, and it can be generated programmatically using the SparkSession builder pattern.

### Spark Session vs Spark Context
From Spark 2.0, SparkSession provides a common entry point for a Spark application. It allows you to interface with Spark’s numerous features with a less amount of constructs. Instead of SparkContext, HiveContext, and SQLContext, everything is now within a SparkSession. One aspect of the explanation why SparkSession is preferable over SparkContext in SparkSession Vs SparkContext battle is that __SparkSession unifies all of Spark’s numerous contexts, removing the developer’s need to worry about generating separate contexts__. Apart from this benefit, the Apache Spark developers have attempted to address the issue of numerous users sharing the same SparkContext.

### Spark Connector
In Apache Spark 3.4, Spark Connect introduced a decoupled client-server architecture that allows remote connectivity to Spark clusters using the DataFrame API and unresolved logical plans as the protocol. The separation between client and server allows Spark and its open ecosystem to be leveraged from everywhere.

Spark Connect is a protocol that specifies how a client application can communicate with a remote Spark Server. Clients that implement the Spark Connect protocol can connect and make requests to remote Spark Servers, very similar to how client applications can connect to databases using a JDBC driver - a query spark.table("some_table").limit(5) should simply return the result.

The Spark Connect client translates DataFrame operations into unresolved logical query plans which are encoded using protocol buffers. These are sent to the server using the gRPC framework. gRPC is performant and language agnostic which makes Spark Connect portable.

<p align="center">
    <img src="images/spark-connect-communication.png" alt="Spark Connect Communication" />
</p>

#### Spark Connect workloads are easier to maintain

When you do not use Spark Connect, the client and Spark Driver must run identical software versions. They need the same Java, Scala, and other dependency versions. Suppose you develop a Spark project on your local machine, package it as a JAR file, and deploy it in the cloud to run on a production dataset. You need to build the JAR file on your local machine with the same dependencies used in the cloud. If you compile the JAR file with Scala 2.13, you must also provision the cluster with a Spark JAR compiled with Scala 2.13.

In contrast, Spark Connect decouples the client and the Spark Driver, so you can update the Spark Driver including server-side dependencies without updating the client. This makes Spark projects much easier to maintain. In particular, for pure Python workloads, decoupling Python from the Java dependency on the client improves the overall user experience with Apache Spark.

Spark Connect is a better architecture for running Spark in production settings. It’s more flexible, easier to maintain, and provides a better developer experience.

#### Operational benefits of Spark Connect
With this new architecture, Spark Connect mitigates several multi-tenant operational issues:

**Stability**: Applications that use too much memory will now only impact their own environment as they can run in their own processes. Users can define their own dependencies on the client and don’t need to worry about potential conflicts with the Spark driver.

**Upgradability**: The Spark driver can now seamlessly be upgraded independently of applications, for example to benefit from performance improvements and security fixes. This means applications can be forward-compatible, as long as the server-side RPC definitions are designed to be backwards compatible.

**Debuggability and observability**: Spark Connect enables interactive debugging during development directly from your favorite IDE. Similarly, applications can be monitored using the application’s framework native metrics and logging libraries.


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

### Deploy Mode
This will determine where the Spark Driver will run. In Client mode, Spark runs driver in local machine, and in cluster mode, it runs driver on one of the nodes in the cluster.

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
             --conf spark.sql.warehouse.dir=$PWD/spark-warehouse \ # The default location for managed databases and tables.
             --conf spark.sql.execution.arrow.pyspark.enabled=true
             --conf spark.yarn.maxAppAttempts=1 \ # Number of attempts on executing an application on YARN.
```

## Referencies
- [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)
- [Spark Python API Docs](https://spark.apache.org/docs/latest/api/python/reference)
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
- [Spark Session vs Spark Context](https://www.ksolves.com/blog/big-data/spark/sparksession-vs-sparkcontext-what-are-the-differences)
- [Spark Connect](https://spark.apache.org/spark-connect/)
- [Spark Connect Overview](https://spark.apache.org/docs/3.5.3/spark-connect-overview.html)