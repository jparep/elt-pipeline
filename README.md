# Employee ELT Pipeline

## Setup Instructions

1. **Snowflake**:
   - Run `snowflake/create_employees.sql`, `snowflake/refresh_snowpipe.sql`, and ensure `s3_stage` and `my_s3_integration` are configured (see Snowflake setup scripts).
   - Configure S3 event notifications (SNS → SQS) to trigger Snowpipe auto-ingest.

2. **AWS Lambda**:
   - Deploy `lambda/lambda_function.py` with `lambda/requirements.txt`.
   - Assign an IAM role with S3 read/write permissions.
   - Set up an S3 trigger for the Lambda function.

3. **dbt**:
   - Install dbt: `pip install dbt-snowflake`.
   - Update `dbt_project/profiles/profiles.yml` with Snowflake credentials.
   - Run `dbt deps` and `dbt run` from `dbt_project/`.

4. **Testing**:
   - Use `scripts/update_employee_data.py` to upload sample data to S3.
   - Verify data in `employee_db.employee.employees` and transformed results in `employees`.

## Pipeline Flow
- `scripts/update_employee_data.py` or external source → S3 → Lambda → Snowpipe → Snowflake (raw table) → dbt → Snowflake (transformed table).