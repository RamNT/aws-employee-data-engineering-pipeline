# DynamoDB Audit and Watermark Design

Amazon DynamoDB is used as the control layer for the employee data pipeline.

The pipeline uses two DynamoDB tables:

1. `employee_pipeline_audit`
2. `employee_watermark`

The audit table retains pipeline execution history, while the watermark table stores the latest successful source-processing checkpoint used by the next incremental extraction.

---

## 1. employee_pipeline_audit

The `employee_pipeline_audit` table stores one record for each pipeline execution.

### Partition Key

`batch_id`

Each historical or incremental execution receives a unique batch ID.

Examples:

```text
HIST_001
HIST_002
HIST_003
HIST_004

INCR_001
INCR_002
```

### Typical Attributes

```text
batch_id
pipeline_name
run_type
status
start_timestamp
end_timestamp
dq_status
target_watermark
failure_step
failure_reason
```

### Successful Execution Example

```text
batch_id         = HIST_004
pipeline_name    = employee_pipeline
run_type         = HISTORICAL
status           = SUCCESS
start_timestamp  = <pipeline start timestamp>
end_timestamp    = <pipeline completion timestamp>
dq_status        = PASS
target_watermark = <latest successful updated_at>
```

### Failed Execution Example

```text
batch_id       = HIST_003
pipeline_name  = employee_pipeline
run_type       = HISTORICAL
status         = FAILED
failure_step   = EmployeeSCD2
failure_reason = Redshift employee SCD2 processing failed
```

Failed executions are retained rather than deleted so that pipeline execution history remains available for troubleshooting and audit purposes.

Conceptually:

```text
HIST_001  FAILED
HIST_002  FAILED
HIST_003  FAILED
HIST_004  SUCCESS
INCR_001  SUCCESS
```

---

## 2. employee_watermark

The `employee_watermark` table stores the latest successful source-processing checkpoint for the employee dataset.

Unlike the audit table, this table does not maintain one item for every execution. The same employee control item is updated after each successful pipeline run.

### Partition Key

`table_name`

For this pipeline:

```text
table_name = employee
```

### Watermark Column

The source watermark column is:

```text
updated_at
```

### Typical Attributes

```text
table_name
watermark_column
last_success_watermark
last_batch_id
last_run_type
updated_timestamp
```

### Example Structure

```text
table_name              = employee
watermark_column        = updated_at
last_success_watermark  = <latest successful source timestamp>
last_batch_id           = <latest successful batch ID>
last_run_type           = HISTORICAL or INCREMENTAL
updated_timestamp       = <pipeline completion timestamp>
```

### Purpose

The watermark answers this question:

> From which source timestamp should the next incremental extraction continue?

Apache Airflow reads `last_success_watermark` before the next incremental source extraction.

The source query logically applies:

```sql
WHERE updated_at > :last_success_watermark
```

Only records newer than the previous successful watermark are extracted into the Bronze incremental layer.

---

## 3. Watermark Update Rule

The most important rule in the incremental pipeline is:

> The watermark is updated only after downstream processing completes successfully and Curated data-quality validation returns `PASS`.

### Successful Flow

```text
MySQL incremental extraction
        ↓
S3 Bronze
        ↓
S3 Silver
        ↓
Redshift Staging
        ↓
SCD Type 2 Curated processing
        ↓
Data Quality = PASS
        ↓
Get MAX(updated_at)
        ↓
Update employee_watermark
        ↓
Audit = SUCCESS
        ↓
SES Success Notification
```

The new watermark is derived from the successfully processed batch:

```sql
SELECT MAX(updated_at)
FROM staging.stg_employee;
```

The resulting value becomes `last_success_watermark` for the next Airflow incremental extraction.

---

## Failure Behavior

If any downstream step fails:

```text
Pipeline failure
        ↓
Audit = FAILED
        ↓
SES Failure Notification
        ↓
Watermark remains unchanged
```

The failure path deliberately does **not** update `employee_watermark`.

This prevents the source extraction checkpoint from moving forward when the downstream pipeline has not successfully processed the data.

Example:

```text
Previous successful watermark
        =
2026-10-25 10:00:00

Incremental pipeline fails
        ↓

Watermark remains
2026-10-25 10:00:00
```

The next incremental extraction can therefore retry records after the same successful checkpoint instead of potentially skipping data.

---

## Audit vs Watermark

The two DynamoDB tables solve different problems.

### employee_pipeline_audit

Answers:

```text
What happened during a specific pipeline execution?
```

It maintains execution history.

Example:

```text
HIST_004 → SUCCESS
INCR_001 → SUCCESS
INCR_002 → FAILED
INCR_003 → SUCCESS
```

### employee_watermark

Answers:

```text
Where should the next incremental source extraction begin?
```

It stores only the latest successful checkpoint.

---

## End-to-End Watermark Loop

```text
DynamoDB
employee_watermark
        ↓
Read last_success_watermark
        ↓
Apache Airflow
        ↓
MySQL

WHERE updated_at >
last_success_watermark

        ↓
S3 Bronze Incremental
        ↓
AWS Step Functions
        ↓
Glue / Redshift / SCD2
        ↓
DQ PASS
        ↓
MAX(updated_at)
        ↓
Update DynamoDB watermark
        ↓
Next Airflow Incremental Run
```

This creates a controlled loop between the source-ingestion layer and the downstream AWS data pipeline.
