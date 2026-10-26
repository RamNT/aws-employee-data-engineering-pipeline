

# Amazon DynamoDB Control Tables

Amazon DynamoDB is used for two purposes in the Employee Data Engineering Pipeline:

- Maintaining pipeline execution audit history.
- Tracking the last successfully processed source watermark for incremental processing.

## 1. Pipeline Audit Table

**Table name:** `de_pipeline_audit`

This table maintains the execution history of the employee data pipeline. Each execution records its status and processing details for monitoring and troubleshooting.

### Key Information Tracked

| Attribute | Description |
|---|---|
| `run_id` | Unique identifier for the pipeline execution. |
| `run_type` | Execution mode, such as HISTORICAL or INCREMENTAL. |
| `status` | Execution status, such as RUNNING, SUCCEEDED, or FAILED. |
| `start_time` | Pipeline execution start timestamp. |
| `end_time` | Pipeline execution completion timestamp. |
| `record_count_processed` | Number of records processed during the execution. |
| `batch_id` | Identifier of the processed batch. |

The exact attributes depend on the table's implemented item structure.

### Audit History

The audit table preserves execution history, allowing previous successful and failed runs to be reviewed.

Example:

```text
HIST_002  FAILED
HIST_003  FAILED
HIST_004  SUCCEEDED
```

## 2. Source Watermark Control

The watermark control stores the last successfully processed source position or timestamp used by incremental pipeline executions.

### Responsibilities

- Track the latest successfully processed source watermark.
- Supply the watermark for the next incremental extraction.
- Prevent the pipeline from advancing its watermark after an unsuccessful run.
- Support restart and recovery from the last successful processing point.

The watermark should be updated only after the relevant pipeline processing steps have succeeded.

## 3. Pipeline Integration

AWS Step Functions coordinates the audit and watermark updates as part of pipeline execution.

```text
AWS Step Functions
        |
        v
Start Pipeline Execution
        |
        v
Write / Update Audit Record
        |
        v
Process Employee Batch
        |
        v
Validate Pipeline Result
        |
        +------------------+
        |                  |
      SUCCESS             FAILURE
        |                  |
        v                  v
Update Watermark      Record Failure
        |                  |
        v                  v
Mark Audit Success    Mark Audit Failed
```

## 4. Key Design Principles

- **Auditability:** Maintain execution history for troubleshooting and operational monitoring.
- **Incremental processing:** Track the last successfully processed source position.
- **Recovery:** Resume processing from the last successful watermark where supported by the extraction logic.
- **Execution tracking:** Record relevant batch details, timestamps, and status.
- **Reliable watermark management:** Advance the watermark only after successful processing to avoid skipping unprocessed source data.
