from delta import *
from delta.tables import DeltaTable
from pyspark.sql.types import *

policy_schema = StructType([
    StructField("PolicyID", IntegerType(), nullable=False),
    StructField("CustomerID", IntegerType(), nullable=True),
    StructField("AgentID", IntegerType(), nullable=True),
    StructField("CoverageType", StringType(), nullable=True),
    StructField("Premium", DoubleType(), nullable=False),
    StructField("StartDate", DateType(), nullable=False),
    StructField("EndDate", DateType(), nullable=False),
    StructField("Latest", StringType(), nullable=False),
])
policy_new_schema = StructType([
    StructField("PolicyID", IntegerType(), nullable=False),
    StructField("CustomerID", IntegerType(), nullable=True),
    StructField("AgentID", IntegerType(), nullable=True),
    StructField("CoverageType", StringType(), nullable=True),
    StructField("Premium", DoubleType(), nullable=False),
    StructField("StartDate", DateType(), nullable=False),
    StructField("EndDate", DateType(), nullable=False),
])

def merge_to_policy_table(spark, schema, path_to_csv):

    policyTable = DeltaTable.forPath(spark, "./spark-warehouse/policy")
    updates = spark.read.csv(path_to_csv, header=True, schema=schema, sep=",")

    # Rows to INSERT new information of existing policies
    newAddressesToInsert = updates \
        .alias("updates") \
        .join(policyTable.toDF().alias("policies"), "policyid") \
        .where("policies.latest = true AND \
            updates.customerid <> policies.customerid OR \
            updates.agentid <> policies.agentid OR \
            updates.coveragetype <> policies.coveragetype OR \
            updates.premium <> policies.premium OR \
            updates.startdate <> policies.startdate OR \
            updates.enddate <> policies.enddate")

    # Stage the update by unioning two sets of rows
    # 1. Rows that will be inserted in the whenNotMatched clause
    # 2. Rows that will either update the current addresses of existing policies 
    #    or insert the new addresses of new policies
    stagedUpdates = (
        newAddressesToInsert
        .selectExpr("Null as mergeKey", "updates.*") # Rows for 1
        .union(updates.selectExpr("PolicyID as mergeKey", "*")) # Rows for 2
    )

    # Apply SCD Type 2 operation using merge
    policyTable.alias("policies").merge(
        stagedUpdates.alias("staged_updates"),
        "policies.PolicyID = mergeKey") \
    .whenMatchedUpdate(
        condition = "policies.latest = true OR \
                    policies.customerid <> staged_updates.customerid OR \
                    policies.agentid <> staged_updates.agentid OR \
                    policies.coveragetype <> staged_updates.coveragetype OR \
                    policies.premium <> staged_updates.premium OR \
                    policies.startdate <> staged_updates.startdate OR \
                    policies.enddate <> staged_updates.enddate",
        set = {
            "latest": "false"
        }
    ).whenNotMatchedInsert(
        values = {
            "policyid": "staged_updates.PolicyID",
            "customerid": "staged_updates.CustomerID",
            "agentid": "staged_updates.AgentID",
            "coveragetype": "staged_updates.CoverageType",
            "premium": "staged_updates.Premium",
            "startdate": "staged_updates.StartDate",
            "enddate": "staged_updates.EndDate",
            "latest": "true"
        }
    ).execute()