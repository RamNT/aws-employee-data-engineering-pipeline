
# AWS Lambda

This folder contains the AWS Lambda function responsible for performing data-quality (DQ) checks on the curated employee dimension in Amazon Redshift.

## 1. Gold / Curated Data Quality

**Script:** `employee_gold_dq.py`

The Lambda function uses the Amazon Redshift Data API to execute validation queries against the curated employee dimension:

`curated.dim_employee`

## 2. Data Quality Checks

The Lambda function performs the following validations:

| Check | Description | Expected Result |
|---|---|---|
| Employee ID validation | Verify that `emp_id` is not NULL. | No records with a NULL employee ID. |
| SCD Type 2 validation | Ensure an employee does not have more than one current record. | At most one current record per employee. |
| Current flag validation | Verify that `is_current` contains only `Y` or `N`. | No invalid flag values. |

## 3. Data Quality Results

The latest data-quality result is written to:

`dq-results/employee_dq_result.json`

The result captures the outcome of the curated-layer validation checks.

## 4. Step Functions Integration

The Lambda function returns the data-quality result to AWS Step Functions for conditional PASS/FAIL routing.

```text
AWS Step Functions
        |
        v
AWS Lambda
employee_gold_dq.py
        |
        v
Amazon Redshift Data API
        |
        v
curated.dim_employee
        |
        v
Run Data Quality Checks
        |
        v
Write DQ Result to S3
dq-results/employee_dq_result.json
        |
        v
Return PASS / FAIL
        |
        v
Step Functions Choice State
     /          \
   PASS          FAIL
    |              |
 Continue      Failure Handling
 Pipeline      / Alerting
```

## 5. Key Design Principles

- **Automated validation:** Data-quality checks run as part of the pipeline.
- **SCD Type 2 integrity:** Validates the uniqueness of the current employee record.
- **Data integrity:** Checks employee IDs and current-flag values.
- **Auditability:** Stores the latest DQ result as a JSON file.
- **Orchestration:** Integrates with AWS Step Functions for conditional execution based on the validation outcome.

## 6. Technologies Used

- AWS Lambda
- Amazon Redshift Data API
- Amazon Redshift Serverless
- Amazon S3
- AWS Step Functions
- Python
