import pyspark
from delta import *
import os

def deduplicate_and_overwrite_silver_policy(spark):

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