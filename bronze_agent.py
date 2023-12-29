from delta import *
from delta.tables import DeltaTable
from pyspark.sql.types import *

agent_schema = StructType([
    StructField("AgentID", IntegerType(), nullable=False),
    StructField("FirstName", StringType(), nullable=False),
    StructField("LastName", StringType(), nullable=True),
    StructField("Region", StringType(), nullable=True),
    StructField("PhoneNumber", StringType(), nullable=True),
    StructField("LastUpdated", DateType(), nullable=False),
    StructField("Current", StringType(), nullable=False),
])
agent_new_schema = StructType([
    StructField("AgentID", IntegerType(), nullable=False),
    StructField("FirstName", StringType(), nullable=False),
    StructField("LastName", StringType(), nullable=True),
    StructField("Region", StringType(), nullable=True),
    StructField("PhoneNumber", StringType(), nullable=True),
    StructField("LastUpdated", DateType(), nullable=False),
])

def merge_to_agent_table(spark, schema, path_to_csv):

    agentTable = DeltaTable.forPath(spark, "./spark-warehouse/bronze_agent")
    updates = spark.read.csv(path_to_csv, header=True, schema=schema, sep=",")

    # Rows to INSERT new information of existing agents
    newAddressesToInsert = updates \
        .alias("updates") \
        .join(agentTable.toDF().alias("agents"), "agentid") \
        .where("agents.current = true AND \
            updates.region <> agents.region OR \
            updates.phonenumber <> agents.phonenumber")

    # Stage the update by unioning two sets of rows
    # 1. Rows that will be inserted in the whenNotMatched clause
    # 2. Rows that will either update the current addresses of existing agents 
    #    or insert the new addresses of new agents
    stagedUpdates = (
        newAddressesToInsert
        .selectExpr("Null as mergeKey", "updates.*") # Rows for 1
        .union(updates.selectExpr("AgentID as mergeKey", "*")) # Rows for 2
    )

    # Apply SCD Type 2 operation using merge
    agentTable.alias("agents").merge(
        stagedUpdates.alias("staged_updates"),
        "agents.AgentID = mergeKey") \
    .whenMatchedUpdate(
        condition = "agents.current = true AND \
                    agents.region <> staged_updates.region OR \
                    agents.phonenumber <> staged_updates.phonenumber",
        set = {
            "current": "false"
        }
    ).whenNotMatchedInsert(
        values = {
            "agentid": "staged_updates.AgentID",
            "firstname": "staged_updates.FirstName",
            "lastname": "staged_updates.LastName",
            "region": "staged_updates.Region",
            "phonenumber": "staged_updates.Phonenumber",
            "lastupdated": "staged_updates.LastUpdated",
            "current": "true"
        }
    ).execute()