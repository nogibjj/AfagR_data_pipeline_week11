import os
import requests
from pyspark.sql import SparkSession


def extract(
    url="https://raw.githubusercontent.com/acgowda/cfb-recruit-net/525eea9f7a803080e57cee3e8b0cc0dd319ce0d3/data/2020/usc_offers.csv",
    file_path="/dbfs/tmp/usc_offers.csv",  
    timeout=10,
):

    # Ensure the directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    print(f"Starting download from {url}")
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()  # Raise an error for bad status codes
    with open(file_path, "wb") as file:
        file.write(response.content)
    print(f"File downloaded and saved to {file_path}")
    return file_path  # Keep this as is


def load(file_path, spark):

    # Convert the `/dbfs` path to `dbfs:/` for Spark
    spark_path = file_path.replace("/dbfs", "dbfs:")
    print(f"Loading data from {spark_path} into a Spark DataFrame...")
    df = spark.read.csv(spark_path, header=True, inferSchema=True)
    print(f"Data successfully loaded into a DataFrame with {df.count()} rows.")
    return df

def show_as_table(df, table_name="usc_offers"):
    df.createOrReplaceTempView(table_name)
    print(f"Table '{table_name}' created successfully.")
    # Optionally display the first few rows using SQL
    df.show(10)  # Show the first 10 rows of the DataFrame


if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("Players").getOrCreate()

    # Extract the data
    file_path = extract()

    # Load the data into a Spark DataFrame
    df = load(file_path, spark)
    show_as_table(df)

