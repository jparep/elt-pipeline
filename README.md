# Employee ETL Pipeline

## Setup Instructions

1. **Snowflake**:
   - Ensure `employee_db.employee.employees` table exists (see `dbt_project/models/snowflake/create_employees.sql`).
   - Configure Snowflake credentials in `dbt_project/profiles/profiles.yml`.

2. **AWS Lambda**:
   - Deploy `lambda/lambda_function.py` with `lambda/requirements.txt`.
   - Assign an IAM role with S3 read/write and Snowflake access permissions.
   - Set up an S3 trigger for the Lambda function.

3. **dbt**:
   - Install dbt: `pip install dbt-snowflake`.
   - Update `dbt_project/profiles/profiles.yml` with Snowflake credentials.
   - Run `dbt deps` and `dbt run` from `dbt_project/` to validate or summarize data.

4. **Testing**:
   - Use `scripts/update_employee_data.py` to upload sample data to S3.
   - Verify transformed data in `employee_db.employee.employees` and aggregated results in `transformed/employees`.

## Pipeline Flow
- `scripts/update_employee_data.py` or external source → S3 → Lambda (Extract, Transform) → Snowflake (Load) → dbt (Validate/Summarize).