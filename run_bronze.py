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

### initialize data into tables
merge_to_policy_table(spark, policy_schema, "./data/policy.csv")
merge_to_claim_table(spark, "./data/claim.csv")
merge_to_agent_table(spark, agent_schema, "./data/agent.csv")
merge_to_customer_table(spark, customer_schema, "./data/customer.csv")

### merge new data into tables
merge_to_policy_table(spark, policy_new_schema, "./data/policy_new.csv")
merge_to_claim_table(spark, "./data/claim_new.csv")
merge_to_agent_table(spark, agent_new_schema, "./data/agent_new.csv")
merge_to_customer_table(spark, customer_new_schema, "./data/customer_new.csv")