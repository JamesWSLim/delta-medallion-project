import pyspark
from delta import *
import os

builder = pyspark.sql.SparkSession.builder.appName("SCD2-ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

silver_policy = spark.read.format("delta") \
    .load("./spark-warehouse/bronze_policy")

### drop duplicates
silver_policy.dropDuplicates(["policyid", "customerid", "agentid", "coveragetype", "premium", "startdate", "enddate"])

### create table if table doesn't exist, else overwrite table
if os.path.exists("./spark-warehouse/silver_policy"):
    silver_policy.write.format("delta").mode("overwrite").save("./spark-warehouse/silver_policy")
else:
    silver_policy.createOrReplaceTempView("silver_policy")
    spark.sql(
        "CREATE TABLE silver_policy \
        USING delta \
        AS SELECT * FROM silver_policy;"
    )