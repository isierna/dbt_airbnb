import json
import pandas as pd
import os
import re
from connection import *
from snowflake.connector.pandas_tools import write_pandas

def load_test_results():
    #path to run_results.json
    results_path = 'target/run_results.json'

    if not os.path.exists(results_path):
        print("No results path")
    else:
        print("path => " + results_path + " strating the execution")

    #opening results.json file
    with open(results_path) as file:
        results = json.load(file)



    #getting table name from mainfest.json
    manifest_path = 'target/manifest.json'

    #opening manifest.json file
    with open(manifest_path) as file:
        manifests_info = json.load(file)

    nodes = manifests_info['nodes'].items()
    


    tests = []
    for test in results['results']:
        unique_id = test['unique_id']

        #extracting execution date from run_results.json file
        dates = test['timing']
        for date in dates:
            if(date['name']=='execute'):
                detected_at = date['completed_at']
            else:
                detected_at = 'None'

        #extracting model name from manifest node
        for uid, node in nodes:
            if uid == unique_id:
                table_name = node.get('attached_node')
                test_name = node.get('name')
                test_metadata = node.get('test_metadata')
                if test_metadata:
                    kwargs = test_metadata.get('kwargs')
                    model = kwargs.get('model')
                    model1 = ".".join(re.findall(r"'([^']*)'", model))
                if(table_name == None):
                    table_name = node.get('config').get('meta').get('model')
                    if(table_name ==None):
                        table_name = ", ".join(node.get('depends_on').get('nodes'))



        if unique_id.startswith('test'):
            tests.append({
                'test_unique_id' : test['unique_id'],
                'test_name' : test_name,
                'status' : test['status'],
                'detected_at' : detected_at,
                'table_name' : table_name,
                'test_metadata' : model,
                'test_metadata1' : model1
                })
            



    df = pd.DataFrame(tests)

    conn = get_snowflake_connection()

    tup = write_pandas(
        conn,
        df,
        table_name='test_results_from_json',
        auto_create_table=True,
        overwrite=True
    )

    print(tup)



if __name__ == "__main__":
    # conn = get_snowflake_connection()
    # print_connection_info(conn)
    load_test_results()