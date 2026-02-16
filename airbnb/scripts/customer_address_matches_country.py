import pandas as pd
import pycountry
from connection import *
from geopy.geocoders import Nominatim
from time import sleep
from snowflake.connector.pandas_tools import write_pandas


def customers_address_incorrect_country():
    with get_snowflake_connection() as conn:
        #creating dataframe from customers table in snowflake
        sql = "SELECT CUSTOMER_ID, ADDRESS, COUNTRY FROM AIRBNB.RAW.CUSTOMERS"

        df_customers = pd.read_sql_query(sql, conn)

        #creating nominatim object to search for address
        geolocator = Nominatim(user_agent="dq_test_project", timeout=10)

        failures = []

        for index, row in df_customers.iterrows():
            customer_id = row['CUSTOMER_ID']
            address = row['ADDRESS']
            country = row['COUNTRY']
            py_country_code = name_to_code(country)

            location = geolocator.geocode(address, addressdetails=True)
            if location:
                print(location.raw)
                
                addr = location.raw['address']
                add_city = addr.get('city') or addr.get('town') or addr.get('village', 'N/A')
                add_country = addr.get('country', 'N/A')
                add_country_code = addr.get('country_code', 'N/A')

                print("Comparing address:")
                print(f"Actual address from the customers table: {address}")
                print(f"Nominatim city={add_city}, country={add_country}, country_code={add_country_code} ")

                if(py_country_code != add_country_code):
                    failures.append({
                        'CUSTOMER_ID': customer_id,
                        'CUSTOMER_COUNTRY': country,
                        'CUSTOMER_ADDRESS': address,
                        'CUSTOMER_CCODE': py_country_code,
                        'NOM_COUNTRY': add_country,
                        'NOM_CITY': add_city,
                        'NOM_CCODE': add_country_code
                    })
            else:
                failures.append({
                        'CUSTOMER_ID': customer_id,
                        'CUSTOMER_COUNTRY': country,
                        'CUSTOMER_ADDRESS': address,
                        'CUSTOMER_CCODE': py_country_code,
                        'NOM_COUNTRY': 'Not Found',
                        'NOM_CITY': 'Not Found',
                        'NOM_CCODE': 'Not Found'
                    })

            sleep(2)

        #create new table with failures in snowflake for later use in singular test
        df = pd.DataFrame(failures)

        write_pandas(
            conn,
            df,
            table_name='CUSTOMER_ADDRESS_COUNTRY_MISMATCHES',
            auto_create_table=True,
            overwrite=True
        )

#get iso_2 code for country in customers to compare codes with Nominatim
def name_to_code(name):
    CUSTOM_MAPPINGS = {
        'uae':'ae',
        'uk':'gb',
        'hk':'cn',
        'mo':'cn',
    }

    if name.lower() in CUSTOM_MAPPINGS:
        return CUSTOM_MAPPINGS.get(name.lower())
    
    try:
        return pycountry.countries.lookup(name).alpha_2.lower()
    except LookupError:
        return None
       



if __name__ == "__main__":
    customers_address_incorrect_country()
    print("Finished.")
