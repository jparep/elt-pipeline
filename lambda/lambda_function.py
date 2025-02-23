import json
import boto3
import snowflake.connector

# AWS S3 Client
s3_client = boto3.client('s3')

# Snowflake Configuration
SNOWFLAKE_CONFIG = {
    "user": "jparep",
    "password": "Biripian1@1Sf373",
    "account": "kjdoqdf-xu95576",
    "warehouse": "compute_wh",
    "database": "employee_db",
    "schema": "employee"
}

def lambda_handler(event, context):
    """Triggered when a new file is uploaded to S3. Loads data into Snowflake using Snowpipe."""
    try:
        for record in event['Records']:
            bucket_name = record['s3']['bucket']['name']
            object_key = record['s3']['object']['key']

            # Connect to Snowflake
            conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            cursor = conn.cursor()

            # Snowpipe auto-load command
            stage_path = f"@s3_stage/{object_key}"
            copy_sql = f"COPY INTO employees FROM {stage_path} FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1);"
            
            cursor.execute(copy_sql)
            conn.commit()
            conn.close()

            return {"status": "Success", "message": f"Loaded {object_key} into Snowflake"}

    except Exception as e:
        return {"status": "Error", "message": str(e)}
