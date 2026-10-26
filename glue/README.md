
# AWS Glue ETL Jobs

This folder contains the PySpark ETL jobs used in the Employee Data Engineering Pipeline. These jobs transform employee data as it moves from the Bronze layer to the Silver layer and then into Amazon Redshift staging.

## 1. Bronze to Silver

**Script:** `bronze_to_silver.py`

### Responsibilities
- Read the current historical or incremental batch from the S3 Bronze layer.
- Standardize column names.
- Enforce the required Silver-layer data types.
- Trim text fields and standardize text values.
- Remove exact duplicate records.
- Add the pipeline load timestamp.
- Write transformed data to the S3 Silver layer in Parquet format.
- Partition the output by `year/month/day/batch_id`.

## 2. Silver to Redshift Staging

**Script:** `silver_to_redshift_staging.py`

### Responsibilities
- Read only the current batch from the S3 Silver layer.
- Derive `age_band` based on employee age.
- Derive `experience_band` based on years of experience.
- Calculate the SCD Type 2 business hash to identify changes in employee records.
- Perform data-quality checks before loading.
- Load the validated batch into the Amazon Redshift staging table: `staging.stg_employee`.

## 3. Runtime Parameters

Both Glue jobs receive execution-specific parameters from AWS Step Functions. This enables the same ETL jobs to support both historical and incremental processing.

| Parameter | Purpose |
|---|---|
| `run_type` | Identifies the execution mode: historical or incremental. |
| `batch_id` | Uniquely identifies the processing batch. |
| `batch_run_timestamp` | Records the batch execution timestamp. |
| Processing date | Supplies the `year`, `month`, and `day` values used for processing and partitioning. |
| Bronze/Silver paths | Specifies the relevant S3 input and output locations. |

## 4. End-to-End Data Flow

```text
S3 Bronze
    |
    v
AWS Glue: Bronze to Silver
    |
    v
S3 Silver (Partitioned Parquet)
    |
    v
AWS Glue: Silver to Redshift Staging
    |
    v
Amazon Redshift
staging.stg_employee
    |
    v
SCD Type 2 Curated Layer
```

## 5. Key Design Principles

- **Reusability:** The same ETL jobs support historical and incremental executions through runtime parameters.
- **Batch isolation:** Each execution processes only its intended batch.
- **Data standardization:** Column names, data types, and text fields are standardized in the Silver layer.
- **Efficient storage:** Parquet and date-based batch partitioning support efficient downstream processing.
- **Change detection:** The SCD Type 2 business hash helps identify changes in employee attributes.
- **Orchestration:** AWS Step Functions passes runtime parameters and coordinates pipeline execution.
