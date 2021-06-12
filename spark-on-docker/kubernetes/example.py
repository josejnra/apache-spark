from pyspark.sql import SparkSession


def main():
    spark = SparkSession \
        .builder \
        .appName('Example App') \
        .getOrCreate()

    dict_list_data = [{'name': 'Alice', 'id': 1}, 
                      {'name': 'Braga', 'id': 2},
                      {'name': 'Steve', 'id': 2}]
    spark.createDataFrame(dict_list_data).show()


if __name__ == '__main__':
    main()
