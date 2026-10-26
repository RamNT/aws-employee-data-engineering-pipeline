Employee Data Engineering Pipeline
An end-to-end employee data pipeline built with Apache Airflow, Amazon S3, AWS Glue, Amazon Redshift Serverless, AWS Lambda, Amazon DynamoDB, Amazon SES, and AWS Step Functions.
> **Status: Historical and Incremental E2E complete.** Both workflows completed successfully. Incremental SCD Type 2 behavior, Redshift output, DQ, DynamoDB audit/watermark, Step Functions orchestration, and SES notifications were verified.
Architecture
```text
MySQL employee table
        |
        v
Apache Airflow
  |             |
  | HISTORICAL  | INCREMENTAL
  | full load   | updated_at > watermark
  v             v
Amazon S3 Bronze
  bronze/employee/historical/
  bronze/employee/incremental/
        |
        | Step Functions scope
        v
AWS Glue Bronze Crawler
        |
        v
Glue: Bronze -> Silver
        |
        v
S3 Silver Parquet
  year=YYYY/month=MM/day=DD/batch_id=<batch-id>/
        |
        v
AWS Glue Silver Crawler
        |
        v
Glue: Silver -> Redshift Staging
        |
        v
Redshift staging.stg_employee
        |
        v
curated.sp_load_employee_scd2
        |
        v
Redshift curated.dim_employee
        |
        v
AWS Lambda Curated DQ
        |
   +----+----+
   |         |
 PASS       FAIL
   |         |
   v         v
Watermark   Audit FAILED
Audit       SES failure
SUCCESS     Watermark unchanged
SES success
```
Orchestration boundary
Airflow owns MySQL -> S3 Bronze extraction.
Step Functions owns Bronze onward.
Airflow consumes the previous successful watermark.
Step Functions advances the watermark only after curated processing and DQ succeed.
Services Used
Service	Responsibility
MySQL	Employee source system
Apache Airflow	Historical/incremental extraction to S3 Bronze
Amazon S3	Bronze, Silver, DQ output, Redshift temp storage
AWS Glue Crawlers	Bronze/Silver metadata and partition discovery
AWS Glue Data Catalog	S3 metadata catalog
AWS Glue / PySpark	Bronze -> Silver and Silver -> Redshift transformations
Amazon Redshift Serverless	Staging and curated warehouse tables
Redshift stored procedure	Historical initialization and incremental SCD2
AWS Lambda	Curated DQ validation
Amazon DynamoDB	Audit history and current successful watermark
Amazon SES	Success/failure email notifications
AWS Step Functions	AWS-side orchestration and failure routing
Data Layers
Bronze
```text
s3://de-employee-dev-source/bronze/employee/
├── historical/
└── incremental/
```
Silver
```text
s3://de-employee-dev-source/silver/employee/
└── year=YYYY/
    └── month=MM/
        └── day=DD/
            └── batch_id=<batch-id>/
                └── *.parquet
```
Silver is append-based and each execution uses a unique `batch_id`.
Curated / Gold
The logical Gold layer is stored natively in Redshift:
```text
curated.dim_employee
```
V1 intentionally does not maintain a duplicate S3 Gold dataset.
Bronze -> Silver
Production Glue job:
```text
project_employee_bronze_to_silver_prod
```
Runtime arguments:
```text
--run_type
--batch_id
--batch_run_timestamp
--bronze_path
--silver_base_path
--batch_year
--batch_month
--batch_day
```
Transformations:
`id` -> `emp_id`
`name` -> `emp_name`
`department` -> `department_name`
Standardize numeric/date/timestamp types
Trim text fields
Remove exact duplicate rows
Add `load_timestamp`
Write partitioned Parquet by `year/month/day/batch_id`
Silver types include:
```text
emp_id            int
age               int
salary            decimal(12,2)
experience_years  decimal(5,2)
joining_date      date
created_at        timestamp
updated_at        timestamp
load_timestamp    timestamp
```
Silver -> Redshift Staging
Production Glue job:
```text
project_employee_silver_to_redshift_prod
```
The job reads only the current Silver partition and loads:
```text
staging.stg_employee
```
Staging is truncated before each load.
Derived bands
```text
age < 25      -> Young
25-34         -> Early Career
35-44         -> Mid Career
45+           -> Senior
```
```text
0-2 years     -> Fresher
3-7           -> Junior
8-14          -> Experienced
15-24         -> Senior
25+           -> Expert
```
Business Hash
Staging:
```text
stg_business_hash
```
Curated:
```text
tgt_business_hash
```
Tracked business attributes:
```text
emp_name
age
gender
department_name
position
salary
joining_date
experience_years
age_band
experience_band
```
Excluded from SCD change detection:
```text
emp_id
created_at
updated_at
load_timestamp
effective_from
effective_to
is_current
partition fields
batch_id
```
SCD Type 2
Redshift objects:
```text
staging.stg_employee
curated.dim_employee
curated.sp_load_employee_scd2
```
Business key:
```text
emp_id
```
Version fields:
```text
employee_sk
effective_from
effective_to
is_current
```
Historical
Initial records are loaded as:
```text
effective_from = batch_run_timestamp
effective_to   = NULL
is_current     = Y
tgt_business_hash = stg_business_hash
```
The controlled historical initialization clears Curated using:
```sql
DELETE FROM curated.dim_employee;
```
Incremental
Incoming staging rows are compared only to the current curated version:
```text
staging.emp_id = curated.emp_id
AND curated.is_current = 'Y'
```
Classification:
```text
No current emp_id                 -> NEW
emp_id exists + hash differs      -> CHANGED
emp_id exists + hash matches      -> UNCHANGED
```
Actions:
```text
NEW
  -> insert current version

CHANGED
  -> expire old current version
  -> insert new current version

UNCHANGED
  -> no new curated version
```
Changed employee pattern:
```text
Previous version
  is_current   = N
  effective_to = incremental batch timestamp

New version
  is_current     = Y
  effective_from = incremental batch timestamp
  effective_to   = NULL
```
Source-delete handling is out of scope for V1.
Data Quality
Lambda:
```text
project-employee-gold-dq
```
Rules:
`emp_id` must not be NULL.
An employee must not have more than one row where `is_current='Y'`.
`is_current` must contain only `Y` or `N`.
Latest DQ output overwrites:
```text
s3://de-employee-dev-source/dq-results/employee_dq_result.json
```
Audit and Watermark
Audit
DynamoDB table:
```text
employee_pipeline_audit
```
Partition key:
```text
batch_id
```
Typical fields:
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
Watermark
DynamoDB table:
```text
employee_watermark
```
Partition key:
```text
table_name
```
Employee control state:
```text
table_name = employee
watermark_column = updated_at
last_success_watermark
last_batch_id
last_run_type
updated_timestamp
```
Critical rule:
```text
Curated success + DQ PASS -> advance watermark
Any failure               -> watermark unchanged
```
The next Airflow incremental extraction uses:
```sql
WHERE updated_at > :last_success_watermark
```
Step Functions Workflow
Core flow:
```text
InitializeExecution
-> CreateAuditRunning
-> CheckRunType
-> SetHistoricalPath / SetIncrementalPath
-> Start + poll Bronze crawler
-> Run Bronze-to-Silver Glue (.sync)
-> Start + poll Silver crawler
-> Run Silver-to-Redshift Glue (.sync)
-> Execute + poll Redshift SCD2 procedure
-> Run Gold DQ Lambda
-> Check DQ status
-> Query MAX(updated_at)
-> Update watermark
-> Update audit SUCCESS
-> SES success
-> PipelineSucceeded
```
Key orchestration concepts learned
`$.field` is JSONPath into current workflow state.
`field.$` evaluates JSONPath dynamically.
Pass-state `Parameters` is used for dynamic state shaping.
`ResultPath = null` keeps original input and discards an unneeded API result.
Combining original input with a result preserves responses needed later.
Glue `.sync` waits for the job to finish.
Redshift Data API uses an asynchronous pattern:
```text
ExecuteStatement -> Wait -> DescribeStatement -> Choice
```
When a returned value is required:
```text
ExecuteStatement -> DescribeStatement -> GetStatementResult
```
Centralized Failure Handling
Known failure branches converge into:
```text
InvalidRunType
BronzeCrawlerFailed
SilverCrawlerFailed
EmployeeSCD2Failed
DQFailed
WatermarkQueryFailed
        |
        v
UpdateAuditFailed
        |
        v
PrepareFailureEmail
        |
        v
SendFailureEmail
        |
        v
PipelineFailed
```
The failure path records the failed step/reason and does not update the watermark.
E2E Validation Results
Historical E2E: SUCCESS
Verified:
Historical Bronze input processed successfully.
Bronze-to-Silver created an isolated historical Silver partition.
Silver crawler/catalog recognized the partitioned dataset.
Current batch loaded to Redshift staging.
Initial SCD2 baseline populated in `curated.dim_employee`.
Initial curated records were current versions.
Curated DQ returned PASS.
DQ JSON was written to S3.
Employee watermark was created.
DynamoDB audit was updated to SUCCESS.
SES success notification was received.
Step Functions reached `PipelineSucceeded`.
Incremental E2E: SUCCESS
The incremental pipeline also completed successfully end to end using the successful historical state as the baseline.
Verified:
Airflow incremental extraction had already landed the delta in the Bronze incremental path.
Step Functions selected the `INCREMENTAL` branch.
Runtime `batch_id` propagated correctly.
Bronze-to-Silver created a separate incremental Silver partition.
Silver-to-Redshift loaded only the current incremental batch into staging.
`curated.sp_load_employee_scd2` executed its incremental branch.
Sample Redshift records were checked to confirm SCD Type 2 behavior.
Business-hash comparison supported NEW / CHANGED / UNCHANGED classification.
Changed employees retained an expired prior version and a new current version.
Unchanged business records did not require an unnecessary new SCD2 version.
Curated DQ completed successfully.
DynamoDB audit was verified.
The successful watermark advanced for the next Airflow extraction.
SES success notification was received and verified.
Step Functions completed successfully.
This validates the complete loop:
```text
Historical baseline
      |
      v
Successful watermark
      |
      v
Airflow incremental extraction
      |
      v
Incremental Bronze -> Silver -> Staging
      |
      v
SCD2 NEW / CHANGED / UNCHANGED
      |
      v
DQ PASS
      |
      v
Advance watermark
      |
      v
Next incremental cycle
```
Issues Encountered and Resolutions
S3 crawler access
Error: `s3:GetObject AccessDenied`
Fix: Granted the crawler role read access to the Silver employee prefix.
Secrets Manager access
Error: `secretsmanager:GetSecretValue AccessDenied`
Fix: Granted the Glue connection role access to the required secret. The project ultimately reused the existing working `redshift-connection`.
New Glue connection / STS networking
Error: Failed to assume role / VPC access to STS.
Fix: Reused the existing tested Redshift JDBC connection instead of adding unnecessary networking complexity.
Glue production script syntax corruption
Error: `SyntaxError: unterminated string literal`
Fix: Replaced malformed script content with clean Python.
Runtime `batch_id` overwritten by `HIST_001`
Symptom: An execution supplied a new batch ID but Silver still landed under `HIST_001`.
Cause: `InitializeExecution` used static Pass-state `Result` content.
Fix: Switched to dynamic `Parameters`, for example:
```json
"batch_id.$": "$.batch_id"
```
Step Functions Glue permissions
Error: `glue:StartCrawler AccessDenied`
Fix: Granted the Step Functions execution role the crawler APIs used by the workflow, including `glue:StartCrawler` and `glue:GetCrawler`.
Step Functions Redshift Data API permissions
Error: `redshift-data:ExecuteStatement AccessDenied`
Fix: Granted the required Redshift Data API/Serverless actions to the Step Functions execution role.
Lambda Redshift database permission
Error: `permission denied for schema curated`
Fix: Distinguished AWS IAM from Redshift database authorization, identified the IAM-derived Redshift user using `SELECT current_user`, and granted the required Redshift privileges.
Malformed Choice JSONPath
Error: Invalid path containing `String equals STARTED` inside the path text.
Fix: Separated Choice configuration into:
```text
Variable: $.scd2_status.Status
Operator: String equals
Value: STARTED
Next: WaitForEmployeeSCD2
```
Step Functions Redshift database privilege
Error: `permission denied for schema curated`
Fix: Granted the Step Functions IAM-derived Redshift identity appropriate `USAGE`, `SELECT`, `INSERT`, `UPDATE`, `DELETE`, and procedure `EXECUTE` privileges.
Historical TRUNCATE permission
Error: permission denied while `TRUNCATE TABLE curated.dim_employee` executed.
Fix: Replaced historical reset with:
```sql
DELETE FROM curated.dim_employee;
```
Invalid DELETE syntax
Error: syntax error near `TABLE`.
Incorrect:
```sql
DELETE TABLE curated.dim_employee;
```
Correct:
```sql
DELETE FROM curated.dim_employee;
```
SES permission
Error: `ses:SendEmail AccessDenied`
Fix: Granted the Step Functions execution role permission to send using the verified SES identity.
Security / IAM Lessons
Different execution layers require different permissions:
```text
Step Functions execution role
  -> call Glue, Redshift Data API, Lambda, DynamoDB, SES

Glue crawler role
  -> read S3 and update Catalog

Glue job role
  -> read/write S3 and use Redshift connection

Lambda execution role
  -> Redshift Data API, logs, S3 DQ output

IAM-derived Redshift database user
  -> schema/table/procedure privileges inside Redshift
```
AWS IAM permission does not automatically grant Redshift schema/table access.
Project Resources
S3
```text
s3://de-employee-dev-source/bronze/employee/historical/
s3://de-employee-dev-source/bronze/employee/incremental/
s3://de-employee-dev-source/silver/employee/
s3://de-employee-dev-source/dq-results/employee_dq_result.json
s3://de-employee-dev-source/redshift-temp/
```
Glue
```text
Database: project_employee_db
Crawler: project_employee_bronze_crawler
Crawler: project_employee_silver_crawler
Job: project_employee_bronze_to_silver_prod
Job: project_employee_silver_to_redshift_prod
Connection: redshift-connection
```
Redshift Serverless
```text
Namespace: cloud-namespace
Workgroup: cloud-de-wrkgrp-redshift
Database: dev
Staging: staging.stg_employee
Curated: curated.dim_employee
Procedure: curated.sp_load_employee_scd2
```
Lambda
```text
project-employee-gold-dq
```
DynamoDB
```text
employee_pipeline_audit
employee_watermark
```
Step Functions
```text
project-employee-end-to-end-pipeline
```
Status
[x] Historical source ingestion to S3 Bronze
[x] Bronze cataloging
[x] Bronze-to-Silver transformation
[x] Silver Parquet partitioning
[x] Silver cataloging
[x] Silver-to-Redshift staging
[x] Historical SCD2 initialization
[x] Incremental SCD2 processing
[x] Curated DQ validation
[x] DynamoDB audit handling
[x] DynamoDB watermark handling
[x] SES success/failure notifications
[x] Centralized Step Functions failure path
[x] Historical E2E success
[x] Incremental E2E success
[x] Incremental sample SCD2 verification
[x] Incremental Redshift, DQ, audit, watermark, and SES validation
Potential V2 Enhancements
Source-delete handling where required.
Stronger idempotency safeguards for reused `batch_id` values.
Tighter least-privilege IAM policies after development.
Automated NEW / CHANGED / UNCHANGED reconciliation metrics.
CloudWatch dashboards and alarms.
Additional DQ rules and threshold-based controls.
Automated Airflow-to-Step-Functions handoff after Bronze landing.
