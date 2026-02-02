def model(dbt, session):
    dbt.config(
        materialized="table"
    )

    df = dbt.ref("dim_hosts_cleansed")
    return df