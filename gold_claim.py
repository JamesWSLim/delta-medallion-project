import pyspark
from delta import *
import os

builder = pyspark.sql.SparkSession.builder.appName("SCD2-ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

gold_policy = spark.read.format("delta") \
    .load("./spark-warehouse/gold_policy")
gold_policy.createOrReplaceTempView("policy_to_join")

bronze_claim = spark.read.format("delta") \
    .load("./spark-warehouse/bronze_claim")
bronze_claim.createOrReplaceTempView("claim_to_join")

gold_claim = spark.sql(
    """
    SELECT t1.claimid,t1.claimamount,t1.claimdate,
        t2.policyid,t2.coveragetype,t2.premium,t2.startdate,t2.enddate
        t2.customerid,t2.customerfirstname,t2.customerlastname,t2.customerdateofbirth
        t2.customeraddress,t2.customercity,t2.customerregion,t2.customerlastupdated,
        t2.agentid,t2.agentfirstname,t2.agentlastname,t2.agentregion,
        t2.agentphonenumber,t2.agentlastupdated
    FROM claim_to_join t1
    LEFT JOIN policy_to_join t2 on t1.policyid=t2.policyid
    WHERE t1.claimdate <= t2.enddate
    AND t1.claimdate >= t2.startdate
    """
)
gold_claim.show()

### create or overwrite delta table
if os.path.exists("./spark-warehouse/gold_claim"):
    gold_claim.write.format("delta").mode("overwrite").save("./spark-warehouse/gold_claim")
else:
    gold_claim.createOrReplaceTempView("gold_claim")
    spark.sql(
        "CREATE TABLE gold_claim \
        USING delta \
        AS SELECT * FROM gold_claim;"
    )