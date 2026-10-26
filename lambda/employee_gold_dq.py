"""
Employee Curated Data Quality Lambda
Responsibilities:
- Query the Redshift curated employee dimension using Data API
- Validate SCD Type 2 integrity
- Return PASS / FAIL to AWS Step Functions
- Write the latest DQ result JSON to Amazon S3
DQ rules:
1. emp_id must not be NULL
2. An employee must not have multiple current versions
3. is_current must contain only Y or N
Deployment-specific values are supplied through Lambda
environment variables.
"""


# ============================================================

import json
import os
import time
from datetime import datetime, timezone

import boto3


# ------------------------------------------------------------
# 1. AWS clients
# ------------------------------------------------------------

redshift_data = boto3.client("redshift-data")
s3 = boto3.client("s3")


# ------------------------------------------------------------
# 2. Configuration from Lambda environment variables
# ------------------------------------------------------------

REDSHIFT_WORKGROUP = os.environ["REDSHIFT_WORKGROUP"]
REDSHIFT_DATABASE = os.environ["REDSHIFT_DATABASE"]

DQ_BUCKET = os.environ["DQ_BUCKET"]
DQ_KEY = os.environ["DQ_KEY"]


# ------------------------------------------------------------
# 3. Helper function: execute one Redshift scalar query
#
# Each DQ SQL statement returns one COUNT value.
#
# Redshift Data API execution is not treated like a normal
# persistent JDBC connection, so we:
#
#   execute statement
#       ↓
#   obtain statement ID
#       ↓
#   check its status
#       ↓
#   retrieve the result
#
# ------------------------------------------------------------

def execute_count_query(sql):

    response = redshift_data.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=REDSHIFT_DATABASE,
        Sql=sql
    )

    statement_id = response["Id"]

    # Wait for the Redshift statement to reach a terminal state.
    while True:

        status_response = redshift_data.describe_statement(
            Id=statement_id
        )

        status = status_response["Status"]

        if status == "FINISHED":
            break

        if status in ["FAILED", "ABORTED"]:
            error_message = status_response.get(
                "Error",
                "Unknown Redshift query error"
            )

            raise RuntimeError(
                f"Redshift DQ query failed: {error_message}"
            )

        # Short pause before checking again.
        time.sleep(1)


    # Fetch the query result.
    #
    # Our SQL produces exactly one row containing one COUNT.
    result = redshift_data.get_statement_result(
        Id=statement_id
    )

    value = result["Records"][0][0]

    # Redshift Data API returns typed field structures.
    # COUNT(*) is normally returned as a long value.
    if "longValue" in value:
        return int(value["longValue"])

    if "stringValue" in value:
        return int(value["stringValue"])

    raise RuntimeError(
        "Unexpected Redshift COUNT result format."
    )


# ------------------------------------------------------------
# 4. Lambda entry point
#
# Step Functions will eventually supply:
#
#   run_type
#   batch_id
#
# ------------------------------------------------------------

def lambda_handler(event, context):

    run_type = event.get("run_type", "UNKNOWN").upper()
    batch_id = event.get("batch_id", "UNKNOWN")


    # --------------------------------------------------------
    # Validate pipeline input
    # --------------------------------------------------------

    if run_type not in ["HISTORICAL", "INCREMENTAL"]:
        raise ValueError(
            "run_type must be HISTORICAL or INCREMENTAL."
        )


    print("========================================")
    print("GOLD DQ STARTED")
    print("========================================")
    print("Run type :", run_type)
    print("Batch ID :", batch_id)


    # --------------------------------------------------------
    # DQ RULE 1
    #
    # Business key must never be NULL.
    # --------------------------------------------------------

    null_emp_id_count = execute_count_query(
        """
        SELECT COUNT(*)
        FROM curated.dim_employee
        WHERE emp_id IS NULL;
        """
    )


    # --------------------------------------------------------
    # DQ RULE 2
    #
    # SCD2 may contain multiple historical versions for the
    # same employee.
    #
    # But only ONE row may be current for an employee.
    # --------------------------------------------------------

    duplicate_current_emp_id_count = execute_count_query(
        """
        SELECT COUNT(*)
        FROM
        (
            SELECT emp_id
            FROM curated.dim_employee
            WHERE is_current = 'Y'
            GROUP BY emp_id
            HAVING COUNT(*) > 1
        ) duplicate_current;
        """
    )


    # --------------------------------------------------------
    # DQ RULE 3
    #
    # is_current must contain only:
    #
    #   Y = current version
    #   N = historical/expired version
    # --------------------------------------------------------

    invalid_is_current_count = execute_count_query(
        """
        SELECT COUNT(*)
        FROM curated.dim_employee
        WHERE is_current IS NULL
           OR is_current NOT IN ('Y', 'N');
        """
    )


    # --------------------------------------------------------
    # Determine overall DQ status
    #
    # PASS only when ALL DQ counts are zero.
    # --------------------------------------------------------

    if (
        null_emp_id_count == 0
        and duplicate_current_emp_id_count == 0
        and invalid_is_current_count == 0
    ):
        dq_status = "PASS"
    else:
        dq_status = "FAIL"


    # --------------------------------------------------------
    # Construct the result document
    #
    # This same structure will:
    #
    #   1. be written to S3
    #   2. be returned to Step Functions
    #
    # --------------------------------------------------------

    dq_result = {
        "run_type": run_type,
        "batch_id": batch_id,
        "dq_status": dq_status,
        "dq_timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "checks": {
            "null_emp_id_count":
                null_emp_id_count,

            "duplicate_current_emp_id_count":
                duplicate_current_emp_id_count,

            "invalid_is_current_count":
                invalid_is_current_count
        }
    }


    # --------------------------------------------------------
    # Write the DQ result to S3
    #
    # IMPORTANT:
    #
    # DQ_KEY is always:
    #
    #   dq-results/employee_dq_result.json
    #
    # Because every execution uses the same S3 object key,
    # the latest DQ result replaces the previous result.
    #
    # No year/month/day/batch partitions are created.
    # --------------------------------------------------------

    s3.put_object(
        Bucket=DQ_BUCKET,
        Key=DQ_KEY,
        Body=json.dumps(
            dq_result,
            indent=2
        ),
        ContentType="application/json"
    )


    # --------------------------------------------------------
    # Log result for CloudWatch
    # --------------------------------------------------------

    print("========================================")
    print("GOLD DQ RESULT")
    print("========================================")
    print(json.dumps(dq_result, indent=2))


    # --------------------------------------------------------
    # Return result to Step Functions
    #
    # Later Step Functions will inspect dq_status:
    #
    #   PASS -> continue success path
    #   FAIL -> failure/audit/notification path
    # --------------------------------------------------------

    return dq_result

