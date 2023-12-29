import pyspark
from delta import *
import os

def deduplicate_and_overwrite_silver_customer(spark):
    
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