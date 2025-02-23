import json
import boto3
import csv
import pandas as pd
import snowflake.connector
import logging
import os
from datetime import datetime

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize S3 and Snowflake clients
s3 = boto3.client('s3')

# Securely fetch Snowflake credentials from AWS Secrets Manager
def get_snowflake_credentials():
    secrets_client = boto3.client("secretsmanager")
    secret_name = os.getenv("SNOWFLAKE_SECRET_NAME", "snowflake_credentials")

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

def transform_data(df):
    """Transform the DataFrame: clean data, categorize ages, and standardize sex."""
    # Clean and standardize data
    df['first'] = df['first'].str.strip().str.title()
    df['last'] = df['last'].str.strip().str.title()
    df['age'] = pd.to_numeric(df['age'], errors='coerce').fillna(0).astype(int)
    df['sex'] = df['sex'].str.upper().replace({'MALE': 'M', 'FEMALE': 'F'}).fillna('Unknown')

    # Categorize ages
    df['age_group'] = pd.cut(df['age'], bins=[0, 25, 35, 45, float('inf')], 
                            labels=['Under 25', '25-35', '36-45', 'Over 45'], 
                            include_lowest=True)

    # Add load timestamp
    df['load_time'] = datetime.now()

    return df

def load_to_snowflake(df):
    """Load transformed data into Snowflake."""
    try:
        SNOWFLAKE_CONFIG = get_snowflake_credentials()
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()

        # Ensure table exists with correct schema
        create_table_sql = """
        CREATE OR REPLACE TABLE employee_db.employee.employees (
            first STRING,
            last STRING,
            age INT,
            sex STRING,
            age_group STRING,
            load_time TIMESTAMP
        );
        """
        cursor.execute(create_table_sql)

        # Insert transformed data
        insert_sql = """
        INSERT INTO employee_db.employee.employees (first, last, age, sex, age_group, load_time)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        data = [tuple(row) for row in df[['first', 'last', 'age', 'sex', 'age_group', 'load_time']].values]
        cursor.executemany(insert_sql, data)

        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Data loaded into Snowflake successfully.")
    except Exception as e:
        logging.error(f"Error loading to Snowflake: {e}")
        raise

def lambda_handler(event, context):
    """
    AWS Lambda function triggered by S3 upload, performing ETL: Extract, Transform, Load.
    """
    try:
        for record in event["Records"]:
            bucket_name = record["s3"]["bucket"]["name"]
            object_key = record["s3"]["object"]["key"]

            logger.info(f"Processing file: {object_key} from bucket: {bucket_name}")

            # Extract: Read CSV from S3
            response = s3.get_object(Bucket=bucket_name, Key=object_key)
            df = pd.read_csv(response['Body'])

            # Transform: Clean and enrich data
            transformed_df = transform_data(df)

            # Load: Insert into Snowflake
            load_to_snowflake(transformed_df)

            logger.info(f"✅ Completed ETL for {object_key}.")

        return {
            "statusCode": 200,
            "body": json.dumps({"message": f"Processed and loaded {object_key} into Snowflake"})
        }

    except Exception as e:
        logger.error(f"Error in ETL process: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }