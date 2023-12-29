import pyspark
from delta import *
import os

def deduplicate_and_overwrite_silver_claim(spark):
    
    silver_claim = spark.read.format("delta") \
        .load("./spark-warehouse/bronze_claim")

    ### drop duplicates
    silver_claim.dropDuplicates(["claimid", "policyid", "claimamount", "claimdate", "description"])

    ### create table if table doesn't exist, else overwrite table
    if os.path.exists("./spark-warehouse/silver_claim"):
        silver_claim.write.format("delta").mode("overwrite").save("./spark-warehouse/silver_claim")
    else:
        silver_claim.createOrReplaceTempView("silver_claim")
        spark.sql(
            "CREATE TABLE silver_claim \
            USING delta \
            AS SELECT * FROM silver_claim;"
        )