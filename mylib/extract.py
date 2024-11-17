# import requests
# import os
# import json
# import base64
# from dotenv import load_dotenv

# load_dotenv()
# server_hostname = os.getenv("SERVER_HOSTNAME")
# access_token = os.getenv("ACCESS_TOKEN")
# FILESTORE_PATH = "dbfs:/FileStore/AFAGR_DATA_PIPELINE_WEEK11"
# headers = {"Authorization": f"Bearer {access_token}"}
# url = f"https://{server_hostname}/api/2.0"

# def extract(
#     url="https://raw.githubusercontent.com/acgowda/cfb-recruit-net/525eea9f7a803080e57cee3e8b0cc0dd319ce0d3/data/2020/usc_offers.csv",
#     file_path="data/usc_offers.csv",
#     directory="data",
# ):
#     """Extract a url to a file path"""
#     if not os.path.exists(directory):
#         os.makedirs(directory)
#     response = requests.get(url)
#     if response.status_code == 200:
#         with open(file_path, "wb") as f:
#             f.write(response.content)
#         print("File downloaded successfully.")
#     else:
#         print(f"Error {response.status_code}: Unable to download the file.")
#     return file_path


# # Run the function
# if __name__ == "__main__":
#     extract()


import requests
from pyspark.sql import SparkSession


def extract(
    url="https://raw.githubusercontent.com/acgowda/cfb-recruit-net/525eea9f7a803080e57cee3e8b0cc0dd319ce0d3/data/2020/usc_offers.csv",
    file_path="dbfs:/FileStore/AFAGR_DATA_PIPELINE_WEEK11/usc_offers.csv",  # Save the file temporarily in DBFS
    timeout=10,
):
    """
    Downloads a file from a specified URL and saves it to the given file path.

    Args:
        url (str): URL of the file to download.
        file_path (str): Local path to save the downloaded file.
        timeout (int): Timeout for the download request in seconds.

    Returns:
        str: The DBFS-compatible file path for Spark.
    """
    print(f"Starting download from {url}")
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()  # Raise an error for bad status codes
    with open(file_path, "wb") as file:
        file.write(response.content)
    print(f"File downloaded and saved to {file_path}")
    # Return Spark-compatible path
    return file_path.replace("/dbfs", "dbfs:")


def load_data(file_path, spark):
    """
    Loads data from a CSV file into a PySpark DataFrame.

    Args:
        file_path (str): The DBFS-compatible path of the CSV file to load.
        spark (SparkSession): The active Spark session.

    Returns:
        pyspark.sql.DataFrame: A DataFrame containing the CSV data.
    """
    print(f"Loading data from {file_path} into a Spark DataFrame...")
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    print(f"Data successfully loaded into a DataFrame with {df.count()} rows.")
    return df


if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("Players").getOrCreate()

    # Extract the data
    file_path = extract()

    # Load the data into a Spark DataFrame
    df = load_data(file_path, spark)

    # Perform a quick preview of the data
    df.show(5)
