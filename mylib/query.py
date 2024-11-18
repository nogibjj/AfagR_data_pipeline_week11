"""
Query function
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, StringType

def load_data(spark, data="dbfs:/tmp/usc_offers.csv", name="Players"):
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

    return df
    
def query(name):
    """queries using spark sql"""
    
    query = f"""

        SELECT school,  round(avg(ranking),2) as avg_ranking,  count(*) as count 
        FROM {name}
        GROUP BY school
        ORDER BY count DESC

    """
    spark = SparkSession.builder.appName("Run Query").getOrCreate()
    df = load_data(spark)
    df = df.createOrReplaceTempView(name)
    query_result = spark.sql(query).show()

    return query_result


if __name__ == "__main__":
    query("Players")
