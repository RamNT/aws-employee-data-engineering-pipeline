"""
Employee Silver -> Redshift Staging ETL
Reads only the Silver partition associated with the current
pipeline execution.
Responsibilities:
- Derive age and experience bands
- Generate SHA-256 business hash for SCD Type 2 comparison
- Perform pre-staging data-quality checks
- Load the current batch into Redshift staging
SCD Type 2 processing itself is performed by the Redshift
stored procedure after this Glue job completes.
"""

# ============================================================

# GLUE JOB: project_employee_silver_to_redshift_prod
#
# PURPOSE:
#   Read ONLY the Silver batch belonging to the current
#   pipeline execution and load it into Redshift staging.
#
# TRANSFORMATIONS:
#   1. Read current Silver Parquet batch
#   2. Create age_band
#   3. Create experience_band
#   4. Create stg_business_hash
#   5. Perform staging DQ
#   6. TRUNCATE Redshift staging
#   7. Load current batch into staging.stg_employee
#
# IMPORTANT:
#   This job does NOT perform SCD2.
#
#   SCD2 belongs to:
#       curated.sp_load_employee_scd2
# ============================================================


# ------------------------------------------------------------
# 1. Imports
# ------------------------------------------------------------

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext

from pyspark.sql.functions import (
    col,
    when,
    lit,
    concat_ws,
    coalesce,
    sha2
)


# ------------------------------------------------------------
# 2. Read arguments supplied by Step Functions
# ------------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "run_type",
        "batch_id",
        "batch_run_timestamp",
        "silver_base_path",
        "batch_year",
        "batch_month",
        "batch_day"
    ]
)


run_type = args["run_type"].upper()

batch_id = args["batch_id"]

batch_run_timestamp = args["batch_run_timestamp"]

silver_base_path = args["silver_base_path"]

batch_year = args["batch_year"]

batch_month = args["batch_month"]

batch_day = args["batch_day"]


# ------------------------------------------------------------
# 3. Validate run_type
# ------------------------------------------------------------

if run_type not in ["HISTORICAL", "INCREMENTAL"]:
    raise ValueError(
        "Invalid run_type. "
        "Expected HISTORICAL or INCREMENTAL."
    )


# ------------------------------------------------------------
# 4. Initialize Spark / Glue
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
# 5. Construct exact Silver batch path
#
# Example:
#
# silver/employee/
#   year=2026/
#     month=10/
#       day=25/
#         batch_id=HIST_001/
#
# Step Functions supplies every dynamic value.
# ------------------------------------------------------------

silver_batch_path = (
    f"{silver_base_path}"
    f"year={batch_year}/"
    f"month={batch_month}/"
    f"day={batch_day}/"
    f"batch_id={batch_id}/"
)


print("========================================")
print("SILVER -> REDSHIFT JOB STARTED")
print("========================================")
print("Run type            :", run_type)
print("Batch ID            :", batch_id)
print("Batch run timestamp :", batch_run_timestamp)
print("Silver batch path   :", silver_batch_path)


# ------------------------------------------------------------
# 6. Read ONLY the current Silver Parquet batch
# ------------------------------------------------------------

df_silver_batch = (
    spark.read
    .format("parquet")
    .load(silver_batch_path)
)


silver_batch_count = df_silver_batch.count()

print(
    "Silver batch row count:",
    silver_batch_count
)


# ------------------------------------------------------------
# 7. Fail if selected Silver batch is empty
#
# This check happens BEFORE Redshift staging is truncated.
# ------------------------------------------------------------

if silver_batch_count == 0:
    raise ValueError(
        "Current Silver batch contains zero records."
    )


# ------------------------------------------------------------
# 8. Create age_band
#
# age < 25       -> Young
# age 25-34      -> Early Career
# age 35-44      -> Mid Career
# age >= 45      -> Senior
# ------------------------------------------------------------

df_gold_ready = (
    df_silver_batch
    .withColumn(
        "age_band",

        when(
            col("age") < 25,
            lit("Young")
        )

        .when(
            (col("age") >= 25) &
            (col("age") <= 34),
            lit("Early Career")
        )

        .when(
            (col("age") >= 35) &
            (col("age") <= 44),
            lit("Mid Career")
        )

        .otherwise(
            lit("Senior")
        )
    )
)


# ------------------------------------------------------------
# 9. Create experience_band
#
# 0-2    -> Fresher
# 3-7    -> Junior
# 8-14   -> Experienced
# 15-24  -> Senior
# 25+    -> Expert
# ------------------------------------------------------------

df_gold_ready = (
    df_gold_ready
    .withColumn(
        "experience_band",

        when(
            (col("experience_years") >= 0) &
            (col("experience_years") <= 2),
            lit("Fresher")
        )

        .when(
            (col("experience_years") >= 3) &
            (col("experience_years") <= 7),
            lit("Junior")
        )

        .when(
            (col("experience_years") >= 8) &
            (col("experience_years") <= 14),
            lit("Experienced")
        )

        .when(
            (col("experience_years") >= 15) &
            (col("experience_years") <= 24),
            lit("Senior")
        )

        .otherwise(
            lit("Expert")
        )
    )
)


# ------------------------------------------------------------
# 10. SCD2 business columns
#
# Only changes to these values trigger a new version later.
#
# Excluded:
#   emp_id
#   created_at
#   updated_at
#   load_timestamp
#   batch/partition metadata
# ------------------------------------------------------------

scd2_columns = [
    "emp_name",
    "age",
    "gender",
    "department_name",
    "position",
    "salary",
    "joining_date",
    "experience_years",
    "age_band",
    "experience_band"
]


# ------------------------------------------------------------
# 11. Normalize business values before hashing
# ------------------------------------------------------------

hash_input_columns = [
    coalesce(
        col(column_name).cast("string"),
        lit("<NULL>")
    )
    for column_name in scd2_columns
]


# ------------------------------------------------------------
# 12. Calculate staging business hash
#
# Gold stores the same hash as tgt_business_hash.
#
# Incremental SCD2 comparison:
#     stg_business_hash
#          vs
#     tgt_business_hash
# ------------------------------------------------------------

df_staging = (
    df_gold_ready
    .withColumn(
        "stg_business_hash",
        sha2(
            concat_ws(
                "||",
                *hash_input_columns
            ),
            256
        )
    )
)


# ------------------------------------------------------------
# 13. Select only Redshift staging columns
# ------------------------------------------------------------

df_staging = df_staging.select(
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
    "age_band",
    "experience_band",
    "stg_business_hash"
)


# ------------------------------------------------------------
# 14. Pre-staging data-quality checks
#
# An incoming batch must contain:
#   - no NULL emp_id
#   - one row per emp_id
#   - no NULL business hash
# ------------------------------------------------------------

staging_row_count = df_staging.count()

null_emp_id_count = (
    df_staging
    .filter(
        col("emp_id").isNull()
    )
    .count()
)

duplicate_emp_id_group_count = (
    df_staging
    .groupBy("emp_id")
    .count()
    .filter(
        col("count") > 1
    )
    .count()
)

null_hash_count = (
    df_staging
    .filter(
        col("stg_business_hash").isNull()
    )
    .count()
)


print("========================================")
print("PRE-STAGING DQ")
print("========================================")
print("Staging rows       :", staging_row_count)
print("NULL emp_id        :", null_emp_id_count)
print("Duplicate emp_id   :", duplicate_emp_id_group_count)
print("NULL business hash :", null_hash_count)


# ------------------------------------------------------------
# 15. Stop on DQ failure
# ------------------------------------------------------------

if null_emp_id_count > 0:
    raise ValueError(
        "DQ failure: staging contains NULL emp_id."
    )

if duplicate_emp_id_group_count > 0:
    raise ValueError(
        "DQ failure: staging contains duplicate emp_id."
    )

if null_hash_count > 0:
    raise ValueError(
        "DQ failure: staging contains NULL "
        "stg_business_hash."
    )


# ------------------------------------------------------------
# 16. Convert Spark DataFrame to Glue DynamicFrame
# ------------------------------------------------------------

df_staging_dynamic = DynamicFrame.fromDF(
    df_staging,
    glueContext,
    "df_staging_dynamic"
)


# ------------------------------------------------------------
# 17. Redshift configuration
# ------------------------------------------------------------

redshift_connection_name = "redshift-connection"
redshift_database = "dev"
redshift_staging_table = "staging.stg_employee"

redshift_temp_path = (
    "s3://<YOUR_BUCKET_NAME>/"
    "redshift-temp/"
)


# ------------------------------------------------------------
# 18. Load current batch into Redshift staging
#
# preactions truncates only staging before loading this batch.
# curated.dim_employee is not modified by this Glue job.
# ------------------------------------------------------------

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=df_staging_dynamic,
    catalog_connection=redshift_connection_name,
    connection_options={
        "dbtable": redshift_staging_table,
        "database": redshift_database,
        "preactions": (
            "TRUNCATE TABLE staging.stg_employee;"
        )
    },
    redshift_tmp_dir=redshift_temp_path,
    transformation_ctx="write_redshift_staging"
)


# ------------------------------------------------------------
# 19. Completion logging
# ------------------------------------------------------------

print("========================================")
print("SILVER -> REDSHIFT COMPLETED")
print("========================================")
print("Run type :", run_type)
print("Batch ID :", batch_id)
print("Rows sent:", staging_row_count)
print("Target   :", redshift_staging_table)


# ------------------------------------------------------------
# 20. Complete the Glue job lifecycle
# ------------------------------------------------------------

job.commit()
