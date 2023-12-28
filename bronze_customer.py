from delta import *
import pyspark
from delta.tables import DeltaTable
from pyspark.sql.types import *

customer_schema = StructType([
    StructField("CustomerID", IntegerType(), nullable=False),
    StructField("FirstName", StringType(), nullable=True),
    StructField("LastName", StringType(), nullable=True),
    StructField("DateOfBirth", StringType(), nullable=True),
    StructField("Address", StringType(), nullable=True),
    StructField("City", StringType(), nullable=True),
    StructField("Region", StringType(), nullable=True),
    StructField("LastUpdated", DateType(), nullable=True),
    StructField("Current", BooleanType(), nullable=False),
])
customer_new_schema = StructType([
    StructField("CustomerID", IntegerType(), nullable=False),
    StructField("FirstName", StringType(), nullable=True),
    StructField("LastName", StringType(), nullable=True),
    StructField("DateOfBirth", StringType(), nullable=True),
    StructField("Address", StringType(), nullable=True),
    StructField("City", StringType(), nullable=True),
    StructField("Region", StringType(), nullable=True),
    StructField("LastUpdated", DateType(), nullable=True),
])

def merge_to_customer_table(spark, schema, path_to_csv):

    customerTable = DeltaTable.forPath(spark, "./spark-warehouse/customer")
    updates = spark.read.csv(path_to_csv, header=True, schema=schema, sep=",")

    # Rows to INSERT new information of existing customers
    newAddressesToInsert = updates \
        .alias("updates") \
        .join(customerTable.toDF().alias("customers"), "customerid") \
        .where("customers.current = true AND \
            updates.dateofbirth <> customers.dateofbirth OR \
            updates.address <> customers.address OR \
            updates.city <> customers.city OR \
            updates.region <> customers.region")

    # Stage the update by unioning two sets of rows
    # 1. Rows that will be inserted in the whenNotMatched clause
    # 2. Rows that will either update the current addresses of existing customers 
    #    or insert the new addresses of new customers
    stagedUpdates = (
        newAddressesToInsert
        .selectExpr("Null as mergeKey", "updates.*") # Rows for 1
        .union(updates.selectExpr("customerID as mergeKey", "*")) # Rows for 2
    )

    # Apply SCD Type 2 operation using merge
    customerTable.alias("customers").merge(
        stagedUpdates.alias("staged_updates"),
        "customers.CustomerID = mergeKey") \
    .whenMatchedUpdate(
        condition = "customers.current = true AND \
                    customers.dateofbirth <> staged_updates.dateofbirth OR \
                    customers.address <> staged_updates.address OR \
                    customers.city <> staged_updates.city OR \
                    customers.region <> staged_updates.region",
        set = {
            "current": "false"
        }
    ).whenNotMatchedInsert(
        values = {
            "customerid": "staged_updates.CustomerID",
            "firstname": "staged_updates.FirstName",
            "lastname": "staged_updates.LastName",
            "dateofbirth": "staged_updates.DateOfBirth",
            "address": "staged_updates.Address",
            "city": "staged_updates.City",
            "region": "staged_updates.Region",
            "lastupdated": "staged_updates.LastUpdated",
            "current": "true"
        }
    ).execute()