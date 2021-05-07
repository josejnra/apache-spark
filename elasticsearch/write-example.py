from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


def main():
    config = SparkConf() \
                        .setMaster('local[*]') \
                        .setAppName('Write to elasticsearch')

    sc = SparkContext(conf=config)
    spark = SparkSession(sc)

    tuple_list_data = [('Alice', 1), ('Braga', 2), ('Steve', 3)]
    df = spark.createDataFrame(tuple_list_data, ['name', 'id'])

    df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes.wan.only", "true") \
        .option("es.net.ssl", "false") \
        .option("es.nodes", "localhost") \
        .option("es.port", 9200) \
        .option("es.index.auto.create", "true") \
        .option("es.resource", "example/_doc") \
        .mode("overwrite") \
        .save()


if __name__ == '__main__':
    main()
