### goal: join table with customer and agent table

import pyspark
from delta import *
import os

builder = pyspark.sql.SparkSession.builder.appName("SCD2-ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

### load df
bronze_policy = spark.read.format("delta") \
    .load("./spark-warehouse/bronze_policy")
bronze_policy.createOrReplaceTempView("policy_to_join")

bronze_customer = spark.read.format("delta") \
    .load("./spark-warehouse/bronze_customer")
bronze_customer.createOrReplaceTempView("customer_to_join")

bronze_agent = spark.read.format("delta") \
    .load("./spark-warehouse/bronze_agent")
bronze_agent.createOrReplaceTempView("agent_to_join")

### SQL join tables
gold_policy = spark.sql(
    """SELECT t1.policyid,t1.coveragetype,t1.premium,t1.startdate,t1.enddate,
    t2.customerid,t2.firstname as customerfirstname,t2.lastname as customerlastname,t2.dateofbirth as customerdateofbirth,
    t2.address as customeraddress,t2.city as customercity,t2.region as customerregion,t2.lastupdated as customerlastupdated, 
    t3.agentid,t3.firstname as agentfirstname,t3.lastname as agentlastname,t3.region as agentregion, 
    t3.phonenumber as agentphonenumber,t3.lastupdated as agentlastupdated 
    FROM policy_to_join t1 
    LEFT JOIN customer_to_join t2 on t1.customerid=t2.customerid 
    LEFT JOIN agent_to_join t3 on t1.agentid=t3.agentid 
    WHERE t2.current=true 
    AND t3.current=true
    """
)
gold_policy.show()

### create or overwrite delta table
if os.path.exists("./spark-warehouse/gold_policy"):
    gold_policy.write.format("delta").mode("overwrite").save("./spark-warehouse/gold_policy")
else:
    gold_policy.createOrReplaceTempView("gold_policy")
    spark.sql(
        "CREATE TABLE gold_policy \
        USING delta \
        AS SELECT * FROM gold_policy;"
    )