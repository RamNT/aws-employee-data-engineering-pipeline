
# AWS Step Functions

AWS Step Functions orchestrates the AWS-side employee data engineering pipeline, from Bronze-layer processing through curated data-quality validation, pipeline auditing, and final notifications.

## 1. Workflow Overview

The state machine supports the following capabilities:

- Historical executions.
- Incremental executions.
- AWS Glue crawler polling.
- AWS Glue ETL job execution.
- Amazon Redshift asynchronous statement polling.
- Curated-layer data-quality validation.
- DynamoDB watermark updates.
- Pipeline audit updates.
- Amazon SES success and failure notifications.
- Centralized failure handling.

## 2. State Machine Definition

The exported state-machine definition is stored in:

`employee_pipeline_state_machine.json`

This JSON file contains the AWS Step Functions state-machine definition, including workflow states, transitions, service integrations, and execution parameters.

## 3. High-Level Workflow

```text
Execution Input
      |
      v
Determine Run Type
(HISTORICAL / INCREMENTAL)
      |
      v
Start and Monitor Glue Crawler
      |
      v
Run Bronze-to-Silver Glue Job
      |
      v
Run Silver-to-Redshift Staging Job
      |
      v
Execute SCD Type 2 Processing
      |
      v
Run Curated Data Quality Lambda
      |
      v
Update DynamoDB Audit / Watermark
      |
      v
Send SES Notification
      |
      v
Execution Complete
```

Failures are routed through centralized error-handling states for audit updates and failure notifications, where configured.

## 4. Execution Modes

### Historical Execution

Processes the historical employee dataset through the pipeline to initialize the downstream data layers.

### Incremental Execution

Processes new or changed employee data using execution-specific batch information and the configured watermark mechanism.

Both modes use the same state machine, with runtime input determining the execution path.

## 5. AWS Services Used

| AWS Service | Responsibility |
|---|---|
| AWS Step Functions | Orchestrates the end-to-end workflow. |
| AWS Glue Crawler | Updates the metadata catalog for source data. |
| AWS Glue ETL | Transforms Bronze data into Silver and prepares data for Redshift staging. |
| Amazon Redshift | Stores staging and curated employee data and applies SCD Type 2 processing. |
| AWS Lambda | Performs curated-layer data-quality validation. |
| Amazon DynamoDB | Stores pipeline audit information and incremental watermarks. |
| Amazon SES | Sends execution success and failure notifications. |

## 6. Key Design Principles

- **Reusable orchestration:** Supports historical and incremental processing through runtime parameters.
- **Execution monitoring:** Polls asynchronous Glue and Redshift operations until completion.
- **Data quality:** Validates curated employee data before successful completion.
- **Incremental processing:** Uses watermark management to support subsequent batches.
- **Auditability:** Records pipeline execution status and relevant processing metrics.
- **Failure handling:** Supports centralized error handling and failure notifications.
- **Operational visibility:** Uses audit records and email notifications to track pipeline outcomes.
