{{
  config(
    materialized = 'view',
    schema = 'report'
    )
}}

 SELECT
            rr.generated_at,
            tr.test_name AS test_name,
            tr.model_unique_id as model_id,
            tr.table_name as table_name_new,
            PARSE_JSON(t.meta):model::string as meta_model,
            coalesce(tr.table_name, PARSE_JSON(t.meta):model::string) as table_name,
            tr.column_name as column_name,
            tr.tags as dimension,
            t.description as description,
            t.meta as impact,
            tr.status as test_result,
            tr.failures as failure_count,    
            tr.detected_at as detected_at,
            rr.execution_time as execution_time
    from
            {{ source('airbnb', 'elementary_test_results') }} AS tr 
            LEFT JOIN {{ source('airbnb', 'dbt_tests') }} t ON tr.TEST_UNIQUE_ID = t.UNIQUE_ID
            LEFT JOIN {{ source('airbnb', 'dbt_run_results') }} rr ON tr.TEST_UNIQUE_ID = rr.unique_id AND rr.resource_type='test' AND DATE_TRUNC('minute', rr.generated_at::timestamp) = DATE_TRUNC('minute', tr.detected_at)
    where date_trunc('minute', detected_at) = (select max(date_trunc('minute', detected_at)) from AIRBNB.DEV_ELEMENTARY.ELEMENTARY_TEST_RESULTS)
    order by test_name