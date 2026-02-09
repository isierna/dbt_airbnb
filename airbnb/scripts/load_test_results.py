import json
import pandas as pd
import os
import re
from connection import *
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime, timezone

def load_test_results():
    #path to run_results.json
    results_path = 'target/run_results.json'

    if not os.path.exists(results_path):
        print("No results path")
        return
    else:
        print("path => " + results_path + " strating the execution")

    #opening results.json file
    with open(results_path) as file:
        results = json.load(file)
    
    invocation_id = results['metadata']['invocation_id']



    #getting table name from mainfest.json
    manifest_path = 'target/manifest.json'

    #opening manifest.json file
    with open(manifest_path) as file:
        manifests_info = json.load(file)

    nodes = manifests_info['nodes']
    


    tests = []
    for test in results['results']:
        unique_id = None
        failures = None
        execution_time = None

        unique_id = test['unique_id']
        failures = test['failures']
        execution_time = test['execution_time']

        #extracting execution date from run_results.json file
        dates = test['timing']
        for date in dates:
            if(date['name']=='execute'):
                detected_at = date['completed_at']
                break

        table_name = None
        test_name = None
        test_metadata = None
        column_name = None
        dimension = None
        description = None
        impact = None
        model = None
        model_final = None
        table_name_final = None

        #extracting model name from manifest node
        node = nodes.get(unique_id)
        if node:
            table_name = node.get('attached_node')
            test_name = node.get('name')
            test_metadata = node.get('test_metadata')

            if test_metadata:
                kwargs = test_metadata.get('kwargs')
                model = kwargs.get('model')
                model_final = ".".join(re.findall(r"'([^']*)'", model))

            if(table_name == None):
                table_name = node.get('config').get('meta').get('model')
                if(table_name ==None):
                    table_name = ", ".join(node.get('depends_on').get('nodes'))

            if model_final != None:
                table_name_final = model_final
            else:
                table_name_final = table_name    

            table_name_final = table_name_final.rsplit(".", 1)[-1]

            column_name = node.get('column_name')
            dimension = node['tags']
            description = node['meta'].get('description')
            impact = node['meta'].get('impact')




        if unique_id.startswith('test'):
            tests.append({
                'INVOCATION_ID' : invocation_id,
                'GENERATED_AT' : datetime.now(timezone.utc).isoformat(),
                'TEST_UNIQUE_ID' : test['unique_id'],
                'TEST_NAME' : test_name,
                'MODEL' : model_final,
                'MODEL_RAW' : model,
                'TABLE_NAME' :  table_name,
                'TABLE_NAME_FOR_BI' : table_name_final,
                'COLUMN_NAME' : column_name,
                'DIMENSION' : dimension,
                'DESCRIPTION' : description,
                'IMPACT' : impact,
                'TEST_RESULT' : test['status'],
                'FAILURE_COUNT' : failures,
                'DETECTED_AT' : detected_at,
                'EXECUTION_TIME' : execution_time
                })
            



    df = pd.DataFrame(tests)

    #check if dataframe is not empty
    if(df.empty):
        print("No test results in the run_results.json. Maybe you need to run 'dbt test'")
        return

    with get_snowflake_connection() as conn:
        tup = write_pandas(
            conn,
            df,
            table_name='TEST_RESULTS_FROM_JSON',
            auto_create_table=True,
            overwrite=False
        )

    print(tup)



if __name__ == "__main__":
    load_test_results()