import os
import json
import pandas as pd
from connection import *
from snowflake.connector.pandas_tools import write_pandas

def load_source_freshness():
    results_file_location = 'target/sources.json'

    if not os.path.exists(results_file_location):
        print("sources.json is missing in the target. Execute 'dbt source freshness'")
        return
    
    #reading json from file
    with open(results_file_location) as file:
        file_results = json.load(file)

    execution_id = file_results['metadata']['invocation_id']

    results = []
    for result in file_results['results']:
        unique_id = result['unique_id']
        max_loaded_at = result['max_loaded_at']
        snapshotted_at = result['snapshotted_at']
        freshness_time = result['max_loaded_at_time_ago_in_s']
        status = result['status']

        results.append({
            'EXECUTION_ID' : execution_id,
            'UNIQUE_ID' : unique_id,
            'MAX_LOADED_AT' : max_loaded_at,
            'SNAPSHOTTED_AT' : snapshotted_at,
            'SINCE_LAST_UPDATE' : freshness_time,
            'FRESHNESS_STATUS' : status
        })

    df = pd.DataFrame(results)
    # conn = get_snowflake_connection()

    with get_snowflake_connection() as conn:
        write_pandas(
            conn,
            df,
            table_name='FRESHNESS_FROM_JSON',
            auto_create_table=True,
            overwrite=False
        )

    print("Finished")

if __name__ == "__main__":
    load_source_freshness()