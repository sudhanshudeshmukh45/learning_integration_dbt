 
{{ config(
    materialized='table'
) }}
 
WITH dim_date as (
    SELECT
       
    TO_CHAR(DATE_DAY, 'YYYYMMDD') AS CAL_KEY,*
    FROM {{ref("date")}}
)
 
SELECT * FROM dim_date