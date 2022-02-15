from dagster import job, op
from dagster_snowflake import snowflake_resource
import os


os.environ["SNOWFLAKE_ACCOUNT"] = "maa23857-main"
os.environ["SNOWFLAKE_USER"] = "mindsdb"
os.environ["SNOWFLAKE_PASSWORD"] = "o7De.zGlTc]sI7v4py2X"
os.environ["SNOWFLAKE_DATABASE"] = "FIVETRAN_DATABASE"
os.environ["SNOWFLAKE_SCHEMA"] = "DUCDUC_SHOPIFY"
os.environ["SNOWFLAKE_WAREHOUSE"] = "FIVETRAN_WAREHOUSE"
print('________________________________')
print(os.environ["SNOWFLAKE_USER"])
print('________________________________')
@op(required_resource_keys={'snowflake'})
def get_one(context):
    context.resources.snowflake.execute_query('SELECT * from "ORDER"')

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



'''
SNOWFLAKE_HOST = 'maa23857-main.snowflakecomputing.com'
SNOWFLAKE_USER = 'mindsdb'
SNOWFLAKE_PASS = 'o7De.zGlTc]sI7v4py2X'
SNOWFLAKE_DB = 'FIVETRAN_DATABASE'
SNOWFLAKE_ACCOUNT = 'maa23857-main'
SNOWFLAKE_SCHEMA = 'DUCDUC_SHOPIFY'
SNOWFLAKE_WAREHOUSE = 'FIVETRAN_WAREHOUSE'
'''