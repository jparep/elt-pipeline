import boto3
import pandas as pd
import io

# AWS S3 Configuration
S3_BUCKET = "s3-sfbucket"
S3_FILE = "employee.csv"

# Initialize S3 client
s3_client = boto3.client("s3")

# Function to read, update, and upload the file
def append_records_to_s3():
    try:
        # 1️⃣ Read existing file from S3
        print(f"Downloading `{S3_FILE}` from `{S3_BUCKET}`...")
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_FILE)
        existing_data = response["Body"].read().decode("utf-8")

        # Convert CSV content to DataFrame
        df_existing = pd.read_csv(io.StringIO(existing_data))

        # 2️⃣ Create new records to append
        new_records = pd.DataFrame([
            {"id": 106, "name": "Alice Johnson", "department": "Engineering", "salary": 75000},
            {"id": 107, "name": "Bob Martin", "department": "Marketing", "salary": 68000},
            {"id": 108, "name": "Emma Brown", "department": "Sales", "salary": 59000},
            {"id": 109, "name": "Olivia Green", "department": "HR", "salary": 62000},
            {"id": 110, "name": "Liam White", "department": "Finance", "salary": 77000}
        ])

        # 3️⃣ Append new records to existing DataFrame
        df_updated = pd.concat([df_existing, new_records], ignore_index=True)

        # Convert back to CSV format (without index)
        csv_buffer = io.StringIO()
        df_updated.to_csv(csv_buffer, index=False)

        # 4️⃣ Upload updated CSV back to S3
        print(f"Uploading updated `{S3_FILE}` back to `{S3_BUCKET}`...")
        s3_client.put_object(Bucket=S3_BUCKET, Key=S3_FILE, Body=csv_buffer.getvalue())

        print("✅ Successfully appended 5 new records to employee.csv in S3!")

    except Exception as e:
        print(f"❌ Error updating `employee.csv`: {e}")

# Run the function
if __name__ == "__main__":
    append_records_to_s3()
