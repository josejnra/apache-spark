from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('My App') \
        .getOrCreate()

    dict_list_data = [{'name': 'Alice', 'id': 1}, {'name': 'Braga', 'id': 2}, {'name': 'Steve', 'id': 2}]
    spark.createDataFrame(dict_list_data).show()

    import time
    time.sleep(60)
