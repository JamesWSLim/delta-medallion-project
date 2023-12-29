import pyspark
from delta import *
import os

def deduplicate_and_overwrite_silver_agent(spark):
    
    silver_agent = spark.read.format("delta") \
        .load("./spark-warehouse/bronze_agent")

    ### drop duplicates
    silver_agent.dropDuplicates(["agentid", "firstname", "lastname", "region", "phonenumber", "lastupdated", "current"])

    ### create table if table doesn't exist, else overwrite table
    if os.path.exists("./spark-warehouse/silver_agent"):
        silver_agent.write.format("delta").mode("overwrite").save("./spark-warehouse/silver_agent")
    else:
        silver_agent.createOrReplaceTempView("silver_agent")
        spark.sql(
            "CREATE TABLE silver_agent \
            USING delta \
            AS SELECT * FROM silver_agent;"
        )