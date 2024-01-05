{{
    config(
        materialized= 'table'
    )
}}

select
  distinct
  {{ dbt_utils.generate_surrogate_key(['JOB_TITLE','JOB_FUNCTION','JOB_FAMILY','EEO_CATEGORY','CAREER_BAND','CAREER_LEVEL']) }} as JOB_KEY,
  JOB_TITLE,JOB_FUNCTION,JOB_FAMILY,EEO_CATEGORY,CAREER_BAND,CAREER_LEVEL
from {{ ref('src_active_employees') }}



-- WITH JOB AS (
-- SELECT distinct  JOB_TITLE,JOB_FUNCTION,JOB_FAMILY,EEO_CATEGORY,CAREER_BAND,CAREER_LEVEL
-- FROM
-- {{ref('src_combined')}}
-- )
-- SELECT *,ROW_NUMBER() OVER (ORDER BY JOB_TITLE,JOB_FUNCTION,JOB_FAMILY,EEO_CATEGORY,CAREER_BAND,CAREER_LEVEL) JOB_KEY
-- FROM JOB


