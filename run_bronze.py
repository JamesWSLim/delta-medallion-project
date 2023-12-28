import pyspark
from delta import *

from bronze_agent import *
from bronze_customer import *
from bronze_policy import *
from bronze_claim import *

builder = pyspark.sql.SparkSession.builder.appName("SCD2-ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

add_or_merge_policy_data(spark)
merge_to_claim_table(spark)
add_or_merge_agent_data(spark)
add_or_merge_customer_data(spark)

agent_delta = spark \
        .read.format("delta") \
        .load("./spark-warehouse/agent")
agent_delta.show()
print(DeltaTable.isDeltaTable(spark, "./spark-warehouse/agent"))
customer_delta = spark \
        .read.format("delta") \
        .load("./spark-warehouse/customer")
customer_delta.show()
print(DeltaTable.isDeltaTable(spark, "./spark-warehouse/customer"))
policy_delta = spark \
        .read.format("delta") \
        .load("./spark-warehouse/policy")
policy_delta.show()
print(DeltaTable.isDeltaTable(spark, "./spark-warehouse/policy"))
claim_delta = spark \
        .read.format("delta") \
        .load("./spark-warehouse/claim")
claim_delta.show()
print(DeltaTable.isDeltaTable(spark, "./spark-warehouse/claim"))
