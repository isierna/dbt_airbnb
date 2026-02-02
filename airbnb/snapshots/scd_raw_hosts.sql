{% snapshot scd_raw_hosts %}

{{
  config(
     target_schema='DEV',
     unique_key='id',
     strategy='check',
     check_cols=['name','is_superhost'],
     invalidate_hard_deletes=True
  )
}}

select * from {{ source('airbnb', 'hosts') }}

{% endsnapshot %}