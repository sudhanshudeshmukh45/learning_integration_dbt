{{
    config(
        materialized='incremental',
        unique_key='enrollment_id',
        merge_exclude_columns=['CREATED_AT'],
        full_refresh = false
    )
}}


SELECT * FROM {{ref("src_learning")}}