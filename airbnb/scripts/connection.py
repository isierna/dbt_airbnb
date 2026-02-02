import configparser
import yaml
from pathlib import Path
import snowflake.connector


def get_snowflake_connection():
    profiles_path = Path.cwd() / 'profiles.yml'

    with open(profiles_path) as file:
        profiles = yaml.safe_load(file)

    profile_name = 'airbnb'
    target_name = 'dev_new'

    target = profiles[profile_name]['outputs'][target_name]

    #read password from property file
    config = configparser.ConfigParser()
    config.read('properties/local.properties')
    snowflake_password=config.get('snowflake','password')

    return snowflake.connector.connect(
        account=target['account'],
        user=target['user'],
        password=snowflake_password,
        role=target['role'],
        warehouse=target['warehouse'],
        database=target['database'],
        schema=target['schema']
    )

def print_connection_info(conn):
    with conn.cursor() as cur:
        cur.execute("""
            select
              current_account(),
              current_user(),
              current_role(),
              current_database(),
              current_schema(),
              current_warehouse()
        """)
        row = cur.fetchone()
    print("Connected OK:")
    print("account,user,role,db,schema,warehouse =", row)