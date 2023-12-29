import pyspark
from delta import *

from silver_agent import *
from silver_customer import *
from silver_policy import *
from silver_claim import *

builder = pyspark.sql.SparkSession.builder.appName("SCD2-ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

### silver level functions
deduplicate_and_overwrite_silver_agent(spark)
deduplicate_and_overwrite_silver_customer(spark)
deduplicate_and_overwrite_silver_policy(spark)
deduplicate_and_overwrite_silver_claim(spark)
