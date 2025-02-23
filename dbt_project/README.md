# Employee ELT Pipeline (dbt Project)

## Setup Instructions

1. Install dbt: `pip install dbt-snowflake`.
2. Update `profiles/profiles.yml` with your Snowflake credentials.
3. Run `dbt deps` to install dependencies, then `dbt run` to execute models.

## Pipeline Flow
- Data is ingested into `employee_db.employee.employees` via Snowpipe.
- dbt transforms raw data into `employees` for analysis.

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
