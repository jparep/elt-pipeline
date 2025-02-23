import json
import boto3
import snowflake.connector
import logging
import os

# Initialize AWS clients
s3_client = boto3.client('s3')

# Secure Snowflake credentials using environment variables
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA")
}

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def lambda_handler(event, context):
    """
    AWS Lambda function triggered when a new CSV file is uploaded to S3.
    - Reads the CSV file
    - Loads the raw data into Snowflake's staging table
    - Triggers dbt for transformations
    """
    try:
        for record in event['Records']:
            bucket_name = record['s3']['bucket']['name']
            object_key = record['s3']['object']['key']

            logger.info(f"Processing file: {object_key} from bucket: {bucket_name}")

            # Connect to Snowflake
            conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            cursor = conn.cursor()

            # Load raw data into Snowflake (using a Snowflake stage)
            stage_path = f"@s3_stage/{object_key}"
            copy_sql = f"""
                COPY INTO employee_db.employee.raw_employees
                FROM {stage_path}
                FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);
            """
            cursor.execute(copy_sql)
            conn.commit()

            logger.info(f"Successfully loaded {object_key} into Snowflake (raw_employees table).")

            # Run dbt to transform data after loading
            trigger_dbt_transformation()

            conn.close()

            return {
                "statusCode": 200,
                "body": json.dumps({"message": f"Loaded {object_key} and triggered dbt transformations."})
            }

    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

def trigger_dbt_transformation():
    """
    Triggers dbt transformation via a command-line execution.
    This function assumes that dbt is installed in an AWS EC2 instance or an AWS Lambda function with container support.
    """
    import subprocess

    try:
        logger.info("Triggering dbt transformation...")
        result = subprocess.run(["dbt", "run"], capture_output=True, text=True)
        logger.info(f"dbt output: {result.stdout}")
    except Exception as e:
        logger.error(f"Failed to trigger dbt: {str(e)}")
