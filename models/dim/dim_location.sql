
{{
    config(
        materialized= 'table'
    )
}}

select
  distinct
  
  {{ dbt_utils.generate_surrogate_key(['WORK_REGION','WORK_COUNTRY','WORK_STATE','WORK_CITY','WORK_LOCATION']) }} as LOCATION_KEY,
  WORK_REGION,WORK_COUNTRY,WORK_STATE,WORK_CITY,WORK_LOCATION
  from {{ ref('src_active_employees') }}




-- WITH location AS (
-- SELECT distinct WORK_REGION,WORK_COUNTRY,WORK_STATE,WORK_CITY,WORK_LOCATION
-- FROM
-- {{ref('src_combined')}}
-- )

-- SELECT *,ROW_NUMBER() OVER (ORDER BY WORK_REGION,WORK_COUNTRY,WORK_STATE,WORK_CITY,WORK_LOCATION) LOCATION_KEY
-- FROM location

