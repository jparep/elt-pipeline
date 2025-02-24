import boto3
import csv
import datetime
import os

# Initialize S3 client
s3 = boto3.client('s3')

BUCKET_NAME = 's3-sfbucket'
PREFIX = 'employee_'

def generate_sample_data():
    return [
        {'first': 'Alice', 'last': 'Brown', 'age': 30, 'sex': 'F'},
        {'first': 'Charlie', 'last': 'Davis', 'age': 40, 'sex': 'M'}
    ]

def upload_to_s3(data):
    """Upload new records to S3 with timestamped filename for optimization."""
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{PREFIX}{timestamp}.csv"
    local_path = f"/tmp/{filename}"
    
    with open(local_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['first', 'last', 'age', 'sex'])
        writer.writeheader()
        writer.writerows(data)
    
    s3.upload_file(local_path, BUCKET_NAME, filename)
    os.remove(local_path)
    print(f"Uploaded {filename} to s3://{BUCKET_NAME}/{filename}")
    return filename

if __name__ == "__main__":
    data = generate_sample_data()
    uploaded_file = upload_to_s3(data)
    print(f"New file {uploaded_file} uploaded. Pipeline will auto-trigger via S3 event.")