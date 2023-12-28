import pyspark
from delta import *
from delta.tables import DeltaTable
from pyspark.sql.types import *

claim_schema = StructType([
    StructField("ClaimID", IntegerType(), nullable=False),
    StructField("PolicyID", IntegerType(), nullable=True),
    StructField("ClaimAmount", DoubleType(), nullable=False),
    StructField("ClaimDate", DateType(), nullable=False),
    StructField("Description", StringType(), nullable=False),
    ])

def merge_to_claim_table(spark, path_to_csv):

    claimTable = DeltaTable.forPath(spark, "./spark-warehouse/claim")
    updates = spark.read.csv(path_to_csv, header=True, schema=claim_schema, sep=",")
    
    claimTable.alias("claims").merge(
        updates.alias("updates"),
        "claims.claimid = updates.claimid") \
    .whenNotMatchedInsertAll() \
    .execute()