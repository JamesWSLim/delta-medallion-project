import pyspark
from delta import *
from pyspark.sql.types import *

builder = pyspark.sql.SparkSession.builder.appName("SCD2-ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

### read data with Change Data Feed
customer_delta_cdf = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .option("endingVersion", 10) \
    .load("./spark-warehouse/customer")

customer_delta_cdf.show()

### read data by version (time travel)
customer_delta_version = spark.read.format("delta") \
    .option("versionAsOf", 1) \
    .load("./spark-warehouse/customer")

customer_delta_version.show()