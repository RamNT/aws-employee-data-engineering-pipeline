# Troubleshooting and Engineering Learnings

This document summarizes the key issues encountered while building and validating the employee data engineering pipeline. Each section records the issue, root cause, resolution, and engineering lesson.

---

## 1. Glue Crawler S3 Access

### Issue

The AWS Glue crawler failed with an Amazon S3 `GetObject` access error.

### Root Cause

The Glue crawler IAM role did not have sufficient permission to read the required S3 objects.

### Resolution

Updated the crawler IAM role to allow read access to the required employee-data S3 prefix.

### Learning

AWS service execution roles require explicit permission to access the resources they operate on.

---

## 2. Glue Secrets Manager Access

### Issue

A Glue Redshift connection could not retrieve the selected AWS Secrets Manager secret.

### Error

```text
secretsmanager:GetSecretValue AccessDenied
```

### Root Cause

The Glue connection IAM role did not have permission to read the selected secret.

### Resolution

Granted `secretsmanager:GetSecretValue` for the required secret. The final pipeline reused an existing successfully tested Redshift Glue JDBC connection rather than introducing redundant connection infrastructure.

### Learning

Having a secret in Secrets Manager does not automatically allow an AWS service to use it. The service execution role must be explicitly authorized to retrieve the secret.

---

## 3. Glue Connection STS / VPC Issue

### Issue

A newly created Glue connection reported that the customer role could not be assumed and referenced VPC access to AWS STS.

### Error

```text
Failed to assume the customers role.
Verify that your VPC has access to STS.
```

### Resolution

Reused the existing working Redshift JDBC connection instead of introducing another connection with additional IAM and networking dependencies.

### Learning

Do not replace proven infrastructure without a requirement. Additional connections can introduce unnecessary VPC, subnet, security-group, credential, and IAM complexity.

---

## 4. Production Glue Script Syntax Failure

### Issue

The Silver-to-Redshift production job failed before executing its transformation logic.

### Error

```text
SyntaxError: unterminated string literal
```

### Root Cause

The copied production script contained malformed characters, quotes, and stray symbols.

### Resolution

Replaced the corrupted section with clean Python code and verified string literals, comparison operators, column names, function arguments, comments, and formatting.

### Learning

Step Functions `.sync` exposed the actual Glue `JobRunState=FAILED` and error message in the workflow execution history, making orchestration-level troubleshooting easier.

---

## 5. Hard-Coded Batch ID

### Issue

A Step Functions execution was started with a new batch ID, but the Silver output was still written under `HIST_001`.

### Root Cause

The `InitializeExecution` Pass state used static `Result` values and overwrote the runtime execution input.

### Resolution

Changed the Pass state to use dynamic `Parameters` with JSONPath:

```json
{
  "batch_id.$": "$.batch_id",
  "run_type.$": "$.run_type",
  "batch_run_timestamp.$": "$.batch_run_timestamp"
}
```

### Learning

In AWS Step Functions:

- `Result` is appropriate for static content.
- `Parameters` with `.$` dynamically evaluates JSONPath expressions.

---

## 6. Step Functions Could Not Start Glue Crawler

### Issue

The Step Functions workflow failed at the Glue crawler start state.

### Error

```text
glue:StartCrawler AccessDenied
```

### Root Cause

The Step Functions execution role did not have permission to call the Glue crawler APIs.

### Resolution

Granted the Step Functions execution role the required Glue permissions, including:

```text
glue:StartCrawler
glue:GetCrawler
```

### Learning

Step Functions service-integration tasks run using the state-machine execution role. That role must be authorized for every AWS API invoked by the workflow.

---

## 7. Redshift Data API Permission

### Issue

Step Functions could not submit the Redshift stored-procedure call.

### Error

```text
redshift-data:ExecuteStatement AccessDenied
```

### Resolution

Granted the Step Functions execution role the Redshift Data API and Redshift Serverless actions required by the workflow:

```text
redshift-data:ExecuteStatement
redshift-data:DescribeStatement
redshift-data:GetStatementResult
redshift-serverless:GetCredentials
```

### Learning

The execution role needs permissions for both submitting asynchronous Redshift queries and retrieving their status and results.

---

## 8. AWS IAM vs Redshift Database Authorization

### Issue

The Redshift Data API call succeeded, but SQL execution failed with a schema-permission error.

### Error

```text
permission denied for schema curated
```

### Root Cause

AWS IAM allowed the API call, but the IAM-derived Redshift database user did not have privileges inside Redshift.

### Resolution

Identified the Redshift database identity through `SELECT current_user` on the same Data API execution path. Granted the required database privileges:

```text
USAGE on staging schema
USAGE on curated schema
SELECT on staging.stg_employee
SELECT, INSERT, UPDATE, DELETE on curated.dim_employee
EXECUTE on curated.sp_load_employee_scd2
```

### Learning

AWS IAM authorization and Redshift database authorization are separate security layers.

```text
AWS IAM
  -> Can the principal call the Redshift Data API?

Redshift privileges
  -> Can the mapped database identity access the schema,
     table, and stored procedure?
```

Both layers must be configured.

---

## 9. Malformed Choice JSONPath

### Issue

A Step Functions Choice state failed because the Variable field contained the path, operator, value, and destination as one string.

### Error

```text
Invalid path '$.scd2_status.Status String equals STARTED ...'
```

### Resolution

Configured each Choice field separately:

```text
Variable: $.scd2_status.Status
Operator: String equals
Value: STARTED
Next: WaitForEmployeeSCD2
```

### Learning

A Choice state's Variable must contain only the JSONPath being evaluated.

---

## 10. Redshift TRUNCATE Permission

### Issue

The historical stored procedure failed while attempting to clear the curated employee table.

### Error

```text
permission denied for relation dim_employee
Where: TRUNCATE TABLE curated.dim_employee
```

### Root Cause

The pipeline's IAM-derived Redshift database identity did not have the authority required for `TRUNCATE`.

### Resolution

For the controlled development dataset, historical reset logic was changed from:

```sql
TRUNCATE TABLE curated.dim_employee;
```

to:

```sql
DELETE FROM curated.dim_employee;
```

### Learning

SQL operations that produce similar data outcomes can have different authorization and operational requirements.

---

## 11. Invalid DELETE Syntax

### Issue

The stored procedure temporarily used invalid SQL syntax.

### Incorrect

```sql
DELETE TABLE curated.dim_employee;
```

### Error

```text
syntax error at or near TABLE
```

### Resolution

Corrected the statement to:

```sql
DELETE FROM curated.dim_employee;
```

### Learning

`DELETE` requires the `FROM` keyword. The corrected stored procedure completed successfully.

---

## 12. SES Send Permission

### Issue

Step Functions reached the notification state but could not send email.

### Error

```text
ses:SendEmail AccessDenied
```

### Root Cause

The Step Functions execution role did not have permission to send email using the verified SES identity.

### Resolution

Granted the Step Functions execution role `ses:SendEmail` for the verified SES identity.

### Learning

Direct Step Functions service integrations use the state-machine execution role. Notification permissions must therefore be assigned to that role.

---

## 13. Secondary Failure During Failure Notification

### Issue

The pipeline correctly entered the centralized failure route, but the failure email itself initially failed because SES permission was missing.

### Resolution

Identified the original failed step through the Step Functions execution history, the failure Pass state, and the DynamoDB audit fields `failure_step` and `failure_reason`. Then separately fixed the SES authorization issue.

### Learning

A visible terminal error may be a secondary failure. Always trace backward to identify the first component that sent the workflow into the failure branch.

---

## 14. Batch Cleanup and Idempotency During Development

### Issue

Failed development runs could leave behind Silver batch partitions, audit records, staging data, or partially completed test state.

### Resolution

Used unique batch IDs for new attempts and cleaned only the artifacts created by a failed run when the same batch ID had to be reused.

### Learning

A batch ID provides isolation and traceability. Reusing a batch ID requires careful cleanup, while unique batch IDs preserve a useful execution history.

---

# Key Engineering Takeaways

- IAM permissions are service specific.
- AWS IAM and Redshift database authorization are independent.
- Runtime parameters should not be hard-coded.
- Batch IDs provide traceability and partition isolation.
- Asynchronous APIs require explicit status polling.
- Glue `.sync` prevents dependent states from running too early.
- Watermarks should advance only after successful downstream processing and DQ.
- SCD Type 2 integrity must be validated, not assumed.
- Centralized failure handling simplifies audit and notification logic.
- Failure notifications can themselves fail and should not hide the original error.
- Pipeline success should be verified through target reconciliation, DQ, audit state, watermark state, and notifications, not only a green workflow execution.
- Retaining failed audit records provides valuable operational history.

---

# Validation Approach

The pipeline was considered complete only after validating:

```text
Historical Step Functions execution = SUCCESS
Incremental Step Functions execution = SUCCESS
Redshift SCD2 sample records = correct
DQ status = PASS
DynamoDB audit = SUCCESS
DynamoDB watermark = advanced correctly
SES notification = received
```

This ensured that both the engineering implementation and the operational controls worked end to end.
