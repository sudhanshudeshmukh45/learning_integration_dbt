-- models/incremental_runs.sql
{{ config(
    materialized='incremental',
    unique_key='table_name'
) }}

SELECT max(updated_at) as LAST_UPDATED_DATE, min(updated_at) as INITIAL_UPDATED_DATE,
'fact_learning' as TABLE_NAME
FROM SUDHANSHU.ACTIVE.FACT_LEARNING 


