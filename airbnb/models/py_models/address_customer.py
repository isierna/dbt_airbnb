import pandas as pd

def model(dbt, session):
    dbt.config(
        materialized="table",
        schema="profiling"
    )

    # Load both as Pandas
    customers = dbt.source('airbnb', 'customers').to_pandas()
    cities = dbt.ref('world_cities').to_pandas()

    # Step 1: Extract city from address
    def extract_city(address):
        if pd.isna(address):
            return None
        parts = [p.strip() for p in address.split(',')]
        if len(parts) >= 2:
            city_mapping = {'NYC': 'New York', 'LA': 'Los Angeles'}
            return city_mapping.get(parts[1], parts[1])
        return None

    customers['EXTRACTED_CITY'] = customers['ADDRESS'].apply(extract_city)

    # Step 2: Find expected country by matching extracted city with seed
    city_to_country = cities.drop_duplicates(subset='CITY')
    city_to_country['CITY_LOWER'] = city_to_country['CITY'].str.lower().str.strip()

    customers['CITY_LOWER'] = customers['EXTRACTED_CITY'].str.lower().str.strip()

    # Step 3: Join to get expected country
    merged = customers.merge(
        city_to_country[['CITY_LOWER', 'COUNTRY']],
        on='CITY_LOWER',
        how='left',
        suffixes=('_ACTUAL', '_EXPECTED')
    )

    # Step 4: Compare countries
    merged['COUNTRIES_MATCH'] = (
        merged['COUNTRY_ACTUAL'].str.lower().str.strip() == 
        merged['COUNTRY_EXPECTED'].str.lower().str.strip()
    )

    # Return mismatches
    result = merged[merged['COUNTRIES_MATCH'] == False][[
        'CUSTOMER_ID', 'ADDRESS', 'EXTRACTED_CITY',
        'COUNTRY_ACTUAL', 'COUNTRY_EXPECTED'
    ]]

    return result