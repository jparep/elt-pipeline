import json
import boto3
import snowflake.connector
import logging
import os

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Securely fetch Snowflake credentials from AWS Secrets Manager
def get_snowflake_credentials():
    secrets_client = boto3.client("secretsmanager")
    secret_name = os.getenv("SNOWFLAKE_SECRET_NAME")

    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret_dict = json.loads(response["SecretString"])
        return {
            "user": secret_dict["SNOWFLAKE_USER"],
            "password": secret_dict["SNOWFLAKE_PASSWORD"],
            "account": secret_dict["SNOWFLAKE_ACCOUNT"],
            "warehouse": secret_dict["SNOWFLAKE_WAREHOUSE"],
            "database": secret_dict["SNOWFLAKE_DATABASE"],
            "schema": secret_dict["SNOWFLAKE_SCHEMA"],
        }
    except Exception as e:
        logging.error(f"Failed to fetch Snowflake credentials: {e}")
        raise

def get_last_load_timestamp():
    """Fetch last Snowflake ingestion timestamp."""
    try:
        SNOWFLAKE_CONFIG = get_snowflake_credentials()
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cur = conn.cursor()

        cur.execute("""
            SELECT MAX(last_load_time) 
            FROM snowflake.account_usage.copy_history 
            WHERE table_name = 'employees';
        """)
        last_load_time = cur.fetchone()[0]

        cur.close()
        conn.close()

        if last_load_time:
            return last_load_time.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return "1970-01-01 00:00:00"

    except Exception as e:
        logging.error(f"Error fetching last load time: {e}")
        return None

def lambda_handler(event, context):
    """
    AWS Lambda function triggered when a new CSV file is uploaded to S3.
    """
    try:
        SNOWFLAKE_CONFIG = get_snowflake_credentials()
        last_load_time = get_last_load_timestamp()
        
        if not last_load_time:
            raise Exception("❌ Could not retrieve last load time.")

        for record in event["Records"]:
            bucket_name = record["s3"]["bucket"]["name"]
            object_key = record["s3"]["object"]["key"]

            logger.info(f"Processing file: {object_key} from bucket: {bucket_name}")

            # Connect to Snowflake
            conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            cursor = conn.cursor()

            # Load only new records using MODIFIED_AFTER
            copy_sql = f"""
                COPY INTO employee_db.employee.employees
                FROM @s3_stage
                FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1)
                MODIFIED_AFTER = '{last_load_time}';
            """
            cursor.execute(copy_sql)
            conn.commit()

            logger.info(f"✅ Loaded new records from {object_key} into Snowflake.")

            # Run dbt transformation
            trigger_dbt_transformation()

            conn.close()

        return {
            "statusCode": 200,
            "body": json.dumps(
                {"message": f"Loaded {object_key} and triggered dbt transformations."}
            ),
        }

    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }

def trigger_dbt_transformation():
    """
    Triggers dbt transformation via command-line execution.
    """
    import subprocess

    try:
        logger.info("🚀 Triggering dbt transformation...")
        result = subprocess.run(["dbt", "run"], capture_output=True, text=True)
        logger.info(f"dbt output: {result.stdout}")
    except Exception as e:
        logger.error(f"Failed to trigger dbt: {str(e)}")
        raise