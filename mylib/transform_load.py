"""
    Transform-Load
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

def example_transform(data_path="dbfs:/tmp/usc_offers.csv",  
    spark=None, ):
    from query import load_data
    """Transforms data by categorizing based on ranking and state"""
    conditions = [
        (col("ranking") >= 0.87),
        (col("state") == "CA") | (col("state") == "TX"),
    ]

    categories = ["High Ranking", "Top States"]
    spark = SparkSession.builder.appName("Run Query").getOrCreate()
    df = load_data(spark)
    df = df.withColumn(
        "Category",
        when(conditions[0], categories[0])
        .when(conditions[1], categories[1])
        .otherwise("Other"),
    )

    return df


def load_transformed(
    df,
    output_path="dbfs:/tmp/usc_offers_transformed.parquet",  
    file_format="parquet",
):
    """
    Saves the transformed DataFrame to the specified output path in the given
    format.
    """
    # Write the DataFrame in the specified format
    if file_format == "parquet":
        df.write.mode("overwrite").parquet(output_path)
    elif file_format == "csv":
        df.write.mode("overwrite").option("header", True).csv(output_path)
    elif file_format == "json":
        df.write.mode("overwrite").json(output_path)
    else:
        raise ValueError("Unsupported file format.Choose'parquet','csv',or'json'.")

    return f"Data saved to {output_path} in {file_format} format."


# Example Usage in Databricks
if __name__ == "__main__":
    # Ensure Spark session is available

    # Transform the dataset
    transformed_df = example_transform()
    save_message = load_transformed(transformed_df)
    print(save_message)