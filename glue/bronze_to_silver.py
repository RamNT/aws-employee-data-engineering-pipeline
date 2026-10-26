"""
Employee Bronze -> Silver ETL
Supports:
- HISTORICAL processing
- INCREMENTAL processing
Runtime parameters are supplied by AWS Step Functions.
Transformations:
- Column standardization
- Type casting
- Text trimming
- Exact duplicate removal
- Load timestamp
- Parquet output
- year/month/day/batch_id partitioning
"""



# ============================================================

# GLUE JOB: project_employee_bronze_to_silver
#
# PURPOSE:
#   Transform one Bronze employee batch into Silver.
#
# SUPPORTED RUN TYPES:
#   HISTORICAL
#   INCREMENTAL
#
# INPUTS FROM STEP FUNCTIONS:
#   --run_type
#   --batch_id
#   --batch_run_timestamp
#   --bronze_path
#   --silver_base_path
#   --batch_year
#   --batch_month
#   --batch_day
#
# TRANSFORMATIONS:
#   1. Read the current Bronze CSV batch
#   2. Rename selected columns
#   3. Enforce Silver data types
#   4. Trim text columns
#   5. Remove exact duplicate records
#   6. Add load_timestamp
#   7. Add partition columns
#   8. Write Silver records as partitioned Parquet
#
# SILVER LAYOUT:
#   silver/employee/
#     year=YYYY/
#       month=MM/
#         day=DD/
#           batch_id=<batch_id>/
#             *.parquet
# ============================================================


# ------------------------------------------------------------
# 1. Import Python, Glue, Spark, and PySpark components
# ------------------------------------------------------------

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    trim,
    lit,
    to_timestamp
)


# ------------------------------------------------------------
# 2. Read runtime arguments
#
# Step Functions passes arguments with leading double hyphens:
#
#   --run_type
#   --batch_id
#
# getResolvedOptions retrieves them without the hyphens:
#
#   args["run_type"]
#   args["batch_id"]
# ------------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "run_type",
        "batch_id",
        "batch_run_timestamp",
        "bronze_path",
        "silver_base_path",
        "batch_year",
        "batch_month",
        "batch_day"
    ]
)


# ------------------------------------------------------------
# 3. Assign runtime arguments to readable variables
# ------------------------------------------------------------

run_type = args["run_type"].upper()

batch_id = args["batch_id"]

batch_run_timestamp = args["batch_run_timestamp"]

bronze_path = args["bronze_path"]

silver_base_path = args["silver_base_path"]

batch_year = args["batch_year"]

batch_month = args["batch_month"]

batch_day = args["batch_day"]


# ------------------------------------------------------------
# 4. Validate run_type
#
# The same job supports both modes.
#
# Step Functions decides the Bronze input path:
#
# HISTORICAL:
#   bronze/employee/historical/
#
# INCREMENTAL:
#   bronze/employee/incremental/
# ------------------------------------------------------------

if run_type not in ["HISTORICAL", "INCREMENTAL"]:
    raise ValueError(
        "Invalid run_type. "
        "Expected HISTORICAL or INCREMENTAL."
    )


# ------------------------------------------------------------
# 5. Initialize Spark and the Glue job
#
# SparkContext:
#   Entry point to the Spark execution engine.
#
# GlueContext:
#   Adds AWS Glue features around Spark.
#
# Job:
#   Manages the AWS Glue job lifecycle.
# ------------------------------------------------------------

sc = SparkContext.getOrCreate()

glueContext = GlueContext(sc)

spark = glueContext.spark_session

job = Job(glueContext)

job.init(
    args["JOB_NAME"],
    args
)


# ------------------------------------------------------------
# 6. Log the execution context
#
# These messages appear in CloudWatch output logs and help
# trace which batch and path the job processed.
# ------------------------------------------------------------

print("========================================")
print("BRONZE -> SILVER JOB STARTED")
print("========================================")
print("Job name            :", args["JOB_NAME"])
print("Run type            :", run_type)
print("Batch ID            :", batch_id)
print("Batch run timestamp :", batch_run_timestamp)
print("Bronze path         :", bronze_path)
print("Silver base path    :", silver_base_path)
print("Batch year          :", batch_year)
print("Batch month         :", batch_month)
print("Batch day           :", batch_day)


# ------------------------------------------------------------
# 7. Read the current Bronze CSV batch
#
# header=true:
#   The first CSV row contains column names.
#
# bronze_path is selected by Step Functions, not hard-coded
# inside this job.
# ------------------------------------------------------------

df_bronze = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .csv(bronze_path)
)


# ------------------------------------------------------------
# 8. Verify that the Bronze batch is not empty
#
# An empty historical/incremental batch should not create an
# empty Silver partition.
# ------------------------------------------------------------

bronze_record_count = df_bronze.count()

print("Bronze record count:", bronze_record_count)

if bronze_record_count == 0:
    raise ValueError(
        "The selected Bronze batch contains zero records. "
        "Silver processing has been stopped."
    )


# ------------------------------------------------------------
# 9. Verify required Bronze columns
#
# If a required column is missing, stop with a clear error
# instead of failing later during transformation.
# ------------------------------------------------------------

required_bronze_columns = [
    "id",
    "name",
    "age",
    "gender",
    "department",
    "position",
    "salary",
    "joining_date",
    "experience_years",
    "created_at",
    "updated_at"
]

missing_columns = [
    column_name
    for column_name in required_bronze_columns
    if column_name not in df_bronze.columns
]

if missing_columns:
    raise ValueError(
        "Required Bronze columns are missing: "
        + ", ".join(missing_columns)
    )


# ------------------------------------------------------------
# 10. Rename Bronze columns to Silver names
#
# Bronze          Silver
# -------         ------
# id              emp_id
# name            emp_name
# department      department_name
# ------------------------------------------------------------

df_silver = (
    df_bronze
    .withColumnRenamed(
        "id",
        "emp_id"
    )
    .withColumnRenamed(
        "name",
        "emp_name"
    )
    .withColumnRenamed(
        "department",
        "department_name"
    )
)


# ------------------------------------------------------------
# 11. Enforce Silver data types
#
# Bronze reflects the source file.
# Silver has controlled, downstream-ready data types.
# ------------------------------------------------------------

df_silver = (
    df_silver

    # Employee identifier
    .withColumn(
        "emp_id",
        col("emp_id").cast("int")
    )

    # Employee age
    .withColumn(
        "age",
        col("age").cast("int")
    )

    # Fixed salary precision
    .withColumn(
        "salary",
        col("salary").cast("decimal(12,2)")
    )

    # Fixed experience precision
    .withColumn(
        "experience_years",
        col("experience_years").cast("decimal(5,2)")
    )

    # Source date string to Spark DATE
    .withColumn(
        "joining_date",
        col("joining_date").cast("date")
    )

    # Source timestamp strings to Spark TIMESTAMP
    .withColumn(
        "created_at",
        col("created_at").cast("timestamp")
    )
    .withColumn(
        "updated_at",
        col("updated_at").cast("timestamp")
    )
)


# ------------------------------------------------------------
# 12. Standardize text columns
#
# trim() removes leading and trailing spaces.
#
# Example:
#   "  Amit Kumar  "
#
# becomes:
#   "Amit Kumar"
#
# No capitalization or spelling changes are performed.
# ------------------------------------------------------------

df_silver = (
    df_silver
    .withColumn(
        "emp_name",
        trim(col("emp_name"))
    )
    .withColumn(
        "gender",
        trim(col("gender"))
    )
    .withColumn(
        "department_name",
        trim(col("department_name"))
    )
    .withColumn(
        "position",
        trim(col("position"))
    )
)


# ------------------------------------------------------------
# 13. Remove exact duplicate records
#
# dropDuplicates() without a column list compares the entire
# business record.
#
# We do NOT use:
#
#   dropDuplicates(["emp_id"])
#
# because SCD2 business processing belongs downstream.
# ------------------------------------------------------------

before_dedup_count = df_silver.count()

df_silver = df_silver.dropDuplicates()

after_dedup_count = df_silver.count()

duplicate_records_removed = (
    before_dedup_count - after_dedup_count
)


print("========================================")
print("BRONZE -> SILVER DEDUPLICATION")
print("========================================")
print(
    "Records before deduplication :",
    before_dedup_count
)
print(
    "Records after deduplication  :",
    after_dedup_count
)
print(
    "Exact duplicates removed     :",
    duplicate_records_removed
)


# ------------------------------------------------------------
# 14. Add Silver load_timestamp
#
# Every row in this batch receives the same execution-level
# timestamp passed by Step Functions.
#
# This preserves one shared batch timestamp across services.
# ------------------------------------------------------------

df_silver = (
    df_silver
    .withColumn(
        "load_timestamp",
        to_timestamp(
            lit(batch_run_timestamp)
        )
    )
)


# ------------------------------------------------------------
# 15. Validate batch_run_timestamp
#
# If the incoming timestamp cannot be parsed, Spark converts
# the value to NULL. Stop before writing bad audit metadata.
# ------------------------------------------------------------

invalid_load_timestamp_count = (
    df_silver
    .filter(
        col("load_timestamp").isNull()
    )
    .count()
)

if invalid_load_timestamp_count > 0:
    raise ValueError(
        "batch_run_timestamp could not be converted into "
        "a Spark timestamp."
    )


# ------------------------------------------------------------
# 16. Add Silver partition columns
#
# These values come from Step Functions.
#
# The partition date represents pipeline processing date,
# not joining_date and not updated_at.
#
# Strings are intentional because Glue Catalog recognizes
# S3 partition path values as partition keys.
# ------------------------------------------------------------

df_silver = (
    df_silver
    .withColumn(
        "year",
        lit(batch_year)
    )
    .withColumn(
        "month",
        lit(batch_month)
    )
    .withColumn(
        "day",
        lit(batch_day)
    )
    .withColumn(
        "batch_id",
        lit(batch_id)
    )
)


# ------------------------------------------------------------
# 17. Select Silver columns in a controlled order
#
# Keeping a defined column order makes the Silver schema
# easier to inspect and maintain.
# ------------------------------------------------------------

df_silver = df_silver.select(
    "emp_id",
    "emp_name",
    "age",
    "gender",
    "department_name",
    "position",
    "salary",
    "joining_date",
    "experience_years",
    "created_at",
    "updated_at",
    "load_timestamp",
    "year",
    "month",
    "day",
    "batch_id"
)


# ------------------------------------------------------------
# 18. Log final Silver schema
# ------------------------------------------------------------

print("========================================")
print("FINAL SILVER SCHEMA")
print("========================================")

df_silver.printSchema()

print(
    "Silver output record count:",
    after_dedup_count
)


# ------------------------------------------------------------
# 19. Write Silver as partitioned Parquet
#
# append:
#   Retains existing Silver batches.
#
# partitionBy:
#   Creates this logical S3 hierarchy:
#
#   year=<value>/
#     month=<value>/
#       day=<value>/
#         batch_id=<value>/
#
# If silver/employee/ does not currently exist, Spark creates
# the required S3 object prefixes when it writes the files.
# ------------------------------------------------------------

(
    df_silver.write
    .mode("append")
    .format("parquet")
    .partitionBy(
        "year",
        "month",
        "day",
        "batch_id"
    )
    .save(silver_base_path)
)


# ------------------------------------------------------------
# 20. Completion logging
# ------------------------------------------------------------

print("========================================")
print("BRONZE -> SILVER JOB COMPLETED")
print("========================================")
print("Run type             :", run_type)
print("Batch ID             :", batch_id)
print("Input record count    :", bronze_record_count)
print("Output record count   :", after_dedup_count)
print("Duplicates removed    :", duplicate_records_removed)
print("Silver destination    :", silver_base_path)


# ------------------------------------------------------------
# 21. Commit the Glue job
#
# job.commit() marks the Glue job lifecycle as successfully
# completed after all processing and writing finishes.
# ------------------------------------------------------------

job.commit()
