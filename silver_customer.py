import pyspark
from delta import *
import os

builder = pyspark.sql.SparkSession.builder.appName("SCD2-ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

silver_customer = spark.read.format("delta") \
    .load("./spark-warehouse/bronze_customer")

### drop duplicates
silver_customer.dropDuplicates(["customerid", "firstname", "lastname", "dateofbirth", "address", "city", "region", "lastupdated", "current"])

### create table if table doesn't exist, else overwrite table
if os.path.exists("./spark-warehouse/silver_customer"):
    silver_customer.write.format("delta").mode("overwrite").save("./spark-warehouse/silver_customer")
else:
    silver_customer.createOrReplaceTempView("silver_customer")
    spark.sql(
        "CREATE TABLE silver_customer \
        USING delta \
        AS SELECT * FROM silver_customer;"
    )