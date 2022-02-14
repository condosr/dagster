from dagster import job, op
from dagster_snowflake import snowflake_resource

@op(required_resource_keys={'snowflake'})
def get_one(context):
    context.resources.snowflake.execute_query('SELECT 1')

@job(resource_defs={'snowflake': snowflake_resource})
def my_snowflake_job():
    get_one()

my_snowflake_job.execute_in_process(
    run_config={
        'resources': {
            'snowflake': {
                'config': {
                    'account': {'env': 'SNOWFLAKE_ACCOUNT'},
                    'user': {'env': 'SNOWFLAKE_USER'},
                    'password': {'env': 'SNOWFLAKE_PASSWORD'},
                    'database': {'env': 'SNOWFLAKE_DATABASE'},
                    'schema': {'env': 'SNOWFLAKE_SCHEMA'},
                    'warehouse': {'env': 'SNOWFLAKE_WAREHOUSE'},
                }
            }
        }
    }
)