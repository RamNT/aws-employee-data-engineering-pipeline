"""
Employee Incremental Ingestion DAG
Purpose:
- Read the latest successful employee watermark.
- Extract source rows with updated_at greater than the
  last successful watermark.
- Write the source delta to the S3 Bronze incremental layer.
The watermark is advanced only after the downstream AWS
pipeline and curated data-quality checks succeed.
"""



from datetime import datetime, timezone
from io import StringIO
import logging

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook


# =========================================================
# Configuration
# =========================================================

MYSQL_CONN_ID = "mysql_conn"
AWS_CONN_ID = "sample_aws_connection"
AWS_REGION = "us-east-1"

DYNAMODB_TABLE = "de_pipeline_watermark"
PIPELINE_NAME = "employee"

S3_BUCKET = "<YOUR_BUCKET>"
S3_PREFIX = "bronze/employee/incremental"


# =========================================================
# Default arguments
# =========================================================

default_args = {
    "owner": "airflow",
}


# =========================================================
# Task 1: Read watermark from DynamoDB
# =========================================================

def read_watermark_from_dynamodb():

    try:
        # Create an AWS session using Airflow's connection.
        aws_hook = AwsBaseHook(
            aws_conn_id=AWS_CONN_ID,
            region_name=AWS_REGION,
        )

        session = aws_hook.get_session(
            region_name=AWS_REGION
        )

        dynamodb = session.resource(
            "dynamodb",
            region_name=AWS_REGION,
        )

        table = dynamodb.Table(DYNAMODB_TABLE)

        logging.info(
            "Reading watermark for pipeline: %s",
            PIPELINE_NAME,
        )

        response = table.get_item(
            Key={
                "pipeline_name": PIPELINE_NAME
            },
            ConsistentRead=True,
        )

        item = response.get("Item")

        if not item:
            raise ValueError(
                f"No watermark item found for pipeline "
                f"'{PIPELINE_NAME}'."
            )

        watermark = item.get(
            "last_successful_watermark"
        )

        if not watermark:
            raise ValueError(
                "last_successful_watermark is missing "
                "from the DynamoDB item."
            )

        # Validate the expected timestamp format.
        datetime.strptime(
            watermark,
            "%Y-%m-%d %H:%M:%S",
        )

        logging.info(
            "Successfully retrieved watermark: %s",
            watermark,
        )

        # Airflow stores this return value in XCom.
        return watermark

    except Exception:
        logging.exception(
            "Failed to read watermark from DynamoDB."
        )
        raise


# =========================================================
# Task 2: Extract incremental records and upload to S3
# =========================================================

def extract_incremental_to_s3(**context):

    try:
        # -------------------------------------------------
        # Retrieve watermark from Task 1 via XCom
        # -------------------------------------------------

        watermark = context["ti"].xcom_pull(
            task_ids="read_watermark_from_dynamodb"
        )

        if not watermark:
            raise ValueError(
                "Watermark was not received from Task 1."
            )

        logging.info(
            "Starting incremental extraction after: %s",
            watermark,
        )

        # -------------------------------------------------
        # Connect to MySQL
        # -------------------------------------------------

        mysql_hook = MySqlHook(
            mysql_conn_id=MYSQL_CONN_ID
        )

        # -------------------------------------------------
        # Query only records updated after the watermark
        # -------------------------------------------------

        sql = """
            SELECT
                id,
                name,
                age,
                gender,
                department,
                position,
                salary,
                joining_date,
                experience_years,
                created_at,
                updated_at
            FROM de_project.employee
            WHERE updated_at > %s
            ORDER BY updated_at, id;
        """

        records = mysql_hook.get_pandas_df(
            sql=sql,
            parameters=(watermark,),
        )

        record_count = len(records)

        logging.info(
            "Incremental records extracted: %s",
            record_count,
        )

        # If nothing changed, do not create an empty file.
        if records.empty:
            logging.info(
                "No incremental records found. "
                "No S3 file will be created."
            )

            raise AirflowSkipException(
                "No incremental records to upload."
            )

        # -------------------------------------------------
        # Calculate the maximum source updated_at
        # -------------------------------------------------

        max_updated_at = records["updated_at"].max()

        if hasattr(max_updated_at, "strftime"):
            max_updated_at = max_updated_at.strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        else:
            max_updated_at = str(max_updated_at)

        # -------------------------------------------------
        # Convert the DataFrame to CSV
        # -------------------------------------------------

        csv_buffer = StringIO()

        records.to_csv(
            csv_buffer,
            index=False,
        )

        csv_data = csv_buffer.getvalue()

        # -------------------------------------------------
        # Generate a unique timestamped S3 key
        # -------------------------------------------------

        timestamp = datetime.now(
            timezone.utc
        ).strftime("%Y%m%dT%H%M%SZ")

        s3_key = (
            f"{S3_PREFIX}/"
            f"employee_incremental_{timestamp}.csv"
        )

        # -------------------------------------------------
        # Upload CSV to the Bronze incremental folder
        # -------------------------------------------------

        s3_hook = S3Hook(
            aws_conn_id=AWS_CONN_ID
        )

        logging.info(
            "Uploading incremental data to S3..."
        )

        s3_hook.load_string(
            string_data=csv_data,
            key=s3_key,
            bucket_name=S3_BUCKET,
            replace=False,
        )

        s3_uri = f"s3://{S3_BUCKET}/{s3_key}"

        logging.info(
            "Incremental upload successful."
        )

        logging.info(
            "Records uploaded: %s",
            record_count,
        )

        logging.info(
            "S3 location: %s",
            s3_uri,
        )

        logging.info(
            "Maximum source updated_at: %s",
            max_updated_at,
        )

        # These details are available to downstream tasks.
        return {
            "record_count": record_count,
            "s3_bucket": S3_BUCKET,
            "s3_key": s3_key,
            "s3_uri": s3_uri,
            "watermark_used": watermark,
            "max_updated_at": max_updated_at,
        }

    except AirflowSkipException:
        raise

    except Exception:
        logging.exception(
            "Incremental extraction or S3 upload failed."
        )
        raise


# =========================================================
# Task 3: Validate S3 bucket and log upload summary
# =========================================================

def validate_s3_upload(**context):

    try:
        upload_result = context["ti"].xcom_pull(
            task_ids="extract_incremental_to_s3"
        )

        if not upload_result:
            raise ValueError(
                "No upload details received from Task 2."
            )

        s3_hook = S3Hook(
            aws_conn_id=AWS_CONN_ID
        )

        bucket_exists = s3_hook.check_for_bucket(
            S3_BUCKET
        )

        if not bucket_exists:
            raise ValueError(
                f"S3 bucket '{S3_BUCKET}' does not exist."
            )

        logging.info(
            "S3 bucket validation successful: %s",
            S3_BUCKET,
        )

        logging.info(
            "Incremental record count: %s",
            upload_result["record_count"],
        )

        logging.info(
            "Uploaded file: %s",
            upload_result["s3_uri"],
        )

        logging.info(
            "Watermark used: %s",
            upload_result["watermark_used"],
        )

    except Exception:
        logging.exception(
            "S3 validation failed."
        )
        raise


# =========================================================
# DAG definition
# =========================================================

with DAG(
    dag_id="employee_incremental_mysql_to_s3",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=[
        "employee",
        "incremental",
        "mysql",
        "s3",
        "bronze",
    ],
) as dag:

    read_watermark_task = PythonOperator(
        task_id="read_watermark_from_dynamodb",
        python_callable=read_watermark_from_dynamodb,
    )

    extract_incremental_task = PythonOperator(
        task_id="extract_incremental_to_s3",
        python_callable=extract_incremental_to_s3,
    )

    validate_s3_task = PythonOperator(
        task_id="validate_s3_upload",
        python_callable=validate_s3_upload,
    )

    # Execute tasks in sequence.
    (
        read_watermark_task
        >> extract_incremental_task
        >> validate_s3_task
    )
