import boto3
import pandas as pd
import io
import os
import requests

# AWS S3 Configuration
S3_BUCKET = "s3-sfbucket"
S3_FILE = "employee.csv"
LAMBDA_FUNCTION_NAME = "sf_lambda"  # Update with your Lambda function name

# Initialize AWS clients
s3_client = boto3.client("s3")
lambda_client = boto3.client("lambda")

def trigger_lambda():
    """
    Invoke AWS Lambda to load new data from S3 into Snowflake.
    """
    try:
        print("🚀 Triggering AWS Lambda to process new data in Snowflake...")
        response = lambda_client.invoke(FunctionName=LAMBDA_FUNCTION_NAME)
        print("✅ AWS Lambda triggered successfully:", response)
    except Exception as e:
        print(f"❌ Failed to trigger AWS Lambda: {e}")

def append_records_to_s3():
    """
    Read employee.csv from S3, append 5 new records, and re-upload.
    """
    try:
        # 1️⃣ Read existing file from S3
        print(f"📥 Downloading `{S3_FILE}` from `{S3_BUCKET}`...")
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_FILE)
        existing_data = response["Body"].read().decode("utf-8")

        # Convert CSV content to DataFrame
        df_existing = pd.read_csv(io.StringIO(existing_data))

        # 2️⃣ Create new records with matching columns
        new_records = pd.DataFrame([
            {"first": "Sam", "last": "Williams", "age": 22, "sex": "Male"},
            {"first": "Emily", "last": "Brown", "age": 21, "sex": "Female"},
            {"first": "Liam", "last": "Taylor", "age": 19, "sex": "Male"},
            {"first": "Sophia", "last": "Anderson", "age": 18, "sex": "Female"},
            {"first": "Michael", "last": "Clark", "age": 23, "sex": "Male"}
        ])

        # 3️⃣ Append new records to existing DataFrame
        df_updated = pd.concat([df_existing, new_records], ignore_index=True)

        # Convert back to CSV format (without index)
        csv_buffer = io.StringIO()
        df_updated.to_csv(csv_buffer, index=False)

        # 4️⃣ Upload updated CSV back to S3
        print(f"📤 Uploading updated `{S3_FILE}` back to `{S3_BUCKET}`...")
        s3_client.put_object(Bucket=S3_BUCKET, Key=S3_FILE, Body=csv_buffer.getvalue())

        print("✅ Successfully appended 5 new records to employee.csv in S3!")

        # 5️⃣ Trigger AWS Lambda after successful upload
        trigger_lambda()

    except Exception as e:
        print(f"❌ Error updating `employee.csv`: {e}")

# Run the function
if __name__ == "__main__":
    append_records_to_s3()
