
from mylib.extract import extract, load
# from mylib.transform_load import example_transform
from mylib.query import  query
import os


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("Players").getOrCreate()
    current_directory = os.getcwd()
    print(current_directory)
    file_path = extract()
    load(file_path, spark)
    query("Players")
