from dagster import job, op
from dagster_snowflake import snowflake_resource
import os
from requests.auth import HTTPBasicAuth
import requests
import dagster
import time


os.environ["SNOWFLAKE_ACCOUNT"] = "maa23857-main"
os.environ["SNOWFLAKE_USER"] = "mindsdb"
os.environ["SNOWFLAKE_PASSWORD"] = "o7De.zGlTc]sI7v4py2X"
os.environ["SNOWFLAKE_DATABASE"] = "FIVETRAN_DATABASE"
os.environ["SNOWFLAKE_SCHEMA"] = "DUCDUC_SHOPIFY"
os.environ["SNOWFLAKE_WAREHOUSE"] = "FIVETRAN_WAREHOUSE"
print('________________________________')
print(os.environ["SNOWFLAKE_USER"])
print('________________________________')


@op(out=dagster.Out(list))
def fivetran_snag(context):
    #onnector_list = []
    api_key = "t7kSmfYtfWTDhdVb"
    api_secret = "6PLNpX9zZsW4nxSX1m7DlgvcfhGIl7Ew"

    # Create Base64 Encoded Basic Auth Header
    auth = HTTPBasicAuth(api_key, api_secret)

    headers = {
    'Authorization': 'Basic ' + api_key,
    'Content-Type': 'application/json'
    }

    limit = 1000
    params = {"limit": limit}
    json_practice ={"url": "https://customer.com/webhook", "events": ["sync_start", "sync_end"], "active": True}

    #api url
    url = "https://api.fivetran.com/v1/groups/overjoyed_lottery/connectors"
    url2 = "https://api.fivetran.com/v1/webhooks/group/overjoyed_lottery"
    url3 = "https://api.fivetran.com/v1/webhooks"
    url4 = "https://api.fivetran.com/v1/webhooks/017e8cd5-4a78-b9fb-b3ad-c0cb387d5059"


    #this is used to create the json object and the data needed exists in the data->items key pairs
    response = requests.get(url=url, auth=auth, params=params).json()
    #response2 = requests.post(url=url2, auth=auth, params=params, json = json_practice).json()
    #response3 = requests.get(url=url3, auth=auth, params=params).json()
    #response4 = requests.delete(url=url4, auth=auth, params=params)
    #print(response)
    #print(response2)
    #print(response3)
    

    #the list of all of the active connectors existing in the overjoyed_lottery group
    connectors = response['data']['items']
    #for connector in connectors:
        #if connector['service'] == 'shopify':
            #connector_list.append(connector['schema'])
    #context.log.info(connector_list)
    customers = []
    for _ in connectors:
        yup = _['schema'].split('_')
        #print(yup)
        for ok in yup:
            if 'f0' in ok and not ok in customers:
                customers.append(ok)
    #print(customers)
    context.log.info(customers)
    return customers


'''
@op(out=dagster.Out(str)) 
def weak_func():
    return('weak')
'''



@op(required_resource_keys={'snowflake'}, ins={'user_list' : dagster.In(list)})
def get_one(context, user_list):
    context.log.info(user_list)
    context.log.info('im here with the query')
    #for _ in connector_list:
    #os.environ["SNOWFLAKE_SCHEMA"] = _
    yup = context.resources.snowflake.execute_query('SELECT * from "ORDER" limit 1', fetch_results = True)
    context.log.info(yup)
    context.log.info('im after the query')

@job(resource_defs={'snowflake': snowflake_resource})
def my_snowflake_job():
    #fill = 'wicked' #fivetran_snag('shopify')
    
    get_one(fivetran_snag())

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