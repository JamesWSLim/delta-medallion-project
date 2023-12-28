import pyspark
from delta import *
from delta.tables import DeltaTable
from pyspark.sql.types import *

builder = pyspark.sql.SparkSession.builder.appName("SCD2-ETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

### create table for policies with CDF
spark.sql(
    "CREATE TABLE policy \
        (policyid INTEGER, \
        customerid INTEGER, \
        agentid INTEGER, \
        coveragetype STRING, \
        premium DOUBLE, \
        startdate DATE, \
        enddate DATE, \
        latest STRING \
        ) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)
### create table for agents with CDF
spark.sql(
    "CREATE TABLE agent \
        (agentid INTEGER, \
        firstname STRING, \
        lastname STRING, \
        region STRING, \
        phonenumber STRING, \
        lastupdated DATE, \
        current STRING \
        ) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)
### create table for customers with CDF
spark.sql(
    "CREATE TABLE customer \
        (customerid INTEGER, \
        firstname STRING, \
        lastname STRING, \
        dateofbirth DATE, \
        address STRING, \
        city STRING, \
        region STRING, \
        lastupdated DATE, \
        current STRING \
        ) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)
### create table for claims with CDF
spark.sql(
    "CREATE TABLE claim \
        (claimid INTEGER, \
        policyid INTEGER, \
        claimamount DOUBLE, \
        claimdate DATE, \
        description STRING \
        ) USING delta TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)