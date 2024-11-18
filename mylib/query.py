from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType

def load_data(spark, data="dbfs:/tmp/usc_offers.csv"):
    """Load data"""
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
    return df

def query(data="dbfs:/tmp/usc_offers.csv", view_name="Players"):
    """Run a query"""
    spark = SparkSession.builder.appName("Run Query").getOrCreate()

    # Load the data
    df = load_data(spark, data)

    # Create a temporary view
    df.createOrReplaceTempView(view_name)

    # Query the data
    query = f"""
        SELECT school, ROUND(AVG(ranking), 2) AS avg_ranking, COUNT(*) AS count
        FROM {view_name}
        GROUP BY school
        ORDER BY count DESC
    """
    result = spark.sql(query)
    result.show()

    return result

if __name__ == "__main__":
    query()