<h1 align="center">SCD2 Delta Medallion Project</h1>

## Description
SCD2 Delta Medallion Project is a ETL pipeline processing Slowly Changing Dimension Type 2 (SCD 2) Dataset with the use of Delta Lake, an open-source framework and Apache Spark written in Python. Goal of this project is to provide a reliable and scalable data lake solution, efficiently handling SCD 2 data ∂type. \
\
Here are some key features from Delta Lake, Apache Spark, and Medallion Architecture implemented in this ETL project: 
1. ACID Transactions: Protect data with serializability and isolation
2. Scalable Metadata: Handle petabyte-scale tables with billions of partitions and files with ease
3. Time Travel: Access/revert to earlier versions of data for audits, rollbacks, or reproduce
4. DML Operations: Supports SQL, Scala/Java and Python APIs to merge(upsert function), update and delete datasets, while upsert function is especially useful for SCD2 datatype
6. Change Data Capture/Feed (CDC): tracks row-level changes between versions for all the data written into tables, which includes row data along with metadata whether the specified row was inserted, deleted, or updated 
7. Integration with Apache Spark: leverage Spark's speed and performance, scalability, and fault tolerance through resilient distributed datasets (RDDs)
8. Schema Evolution / Enforcement: Prevent bad data from causing data corruption
9. Medallion Architecture: Data design pattern used in a lakehouse, with the use of architecture (from Bronze ⇒ Silver ⇒ Gold layer tables) to perform incrementally improving structure to ensure the quality of data going through each layers

<img src="https://cms.databricks.com/sites/default/files/inline-images/building-data-pipelines-with-delta-lake-120823.png">

## Table Schema
![Table Schema](table_schema.png)

## Prerequisites
* Python (using version 3.11.6 or newer)

## Installation
* Create your own virtual environment and [pip](https://pip.pypa.io/en/stable/) to install required libraries in requirement.txt (using Apache Spark version 3.5.0 which is compatible with Delta Lake 3.0.0)
```shell
pip install -r requirements.txt
```