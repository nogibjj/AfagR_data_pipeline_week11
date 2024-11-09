"""
library functions
"""

import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

from pyspark.sql.types import StructType, StructField, FloatType, StringType

LOG_FILE = "pyspark_output.md"


def log_output(operation, output, query=None):
    """adds to a markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"The operation is {operation}\n\n")
        if query:
            file.write(f"The query is {query}\n\n")
        file.write("The truncated output is: \n\n")
        file.write(output)
        file.write("\n\n")


def start_spark(appName):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark


def end_spark(spark):
    spark.stop()
    return "stopped spark session"


def extract(
    url="https://raw.githubusercontent.com/acgowda/cfb-recruit-net/525eea9f7a803080e57cee3e8b0cc0dd319ce0d3/data/2020/usc_offers.csv",
    file_path="data/usc_offers.csv",
    directory="data",
):
    """Extract a url to a file path"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_path, "wb") as f:
            f.write(response.content)
        print("File downloaded successfully.")
    else:
        print(f"Error {response.status_code}: Unable to download the file.")
    return file_path


def load_data(spark, data="data/usc_offers.csv", name="Players"):
    """load data"""
    # data preprocessing by setting schema
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("school", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("ranking", FloatType(), True),
        ]
    )

    df = spark.read.option("header", "true").schema(schema).csv(data)

    log_output("load data", df.limit(10).toPandas().to_markdown())

    return df


def query(spark, df, query, name):
    """queries using spark sql"""
    df = df.createOrReplaceTempView(name)

    log_output("query data", spark.sql(query).toPandas().to_markdown(), query)

    return spark.sql(query).show()


def describe(df):
    summary_stats_str = df.describe().toPandas().to_markdown()
    log_output("describe data", summary_stats_str)

    return df.describe().show()


def example_transform(df):
    """Transforms data by categorizing based on ranking and state"""
    # Define conditions and categories
    conditions = [
        (col("ranking") >= 0.87),
        (col("state") == "CA") | (col("state") == "TX"),
    ]

    categories = ["High Ranking", "Top States"]

    # Apply transformation based on conditions
    df = df.withColumn(
        "Category",
        when(conditions[0], categories[0])
        .when(conditions[1], categories[1])
        .otherwise("Other"),
    )

    log_output("transform data", df.limit(10).toPandas().to_markdown())

    return df.show()
