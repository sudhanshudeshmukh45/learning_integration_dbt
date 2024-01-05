{{
    config(
        materialized = 'incremental',
        unique_key = 'course_key'
    )
}}

select
  distinct
  {{ dbt_utils.generate_surrogate_key(['COURSE_NAME','COURSE','COURSE_TYPE','COURSE_VERSION','LANGUAGE','VENDOR'
]) }} as COURSE_KEY,
  COURSE_NAME, COURSE,
COURSE_TYPE, COURSE_VERSION, LANGUAGE, VENDOR
from {{ ref('src_combined') }}


-- WITH COURSE AS (
-- SELECT distinct  COURSE_NAME, COURSE,
-- COURSE_TYPE, COURSE_VERSION, LANGUAGE, VENDOR
-- FROM
-- {{ref('src_combined')}}
-- )
-- SELECT *,ROW_NUMBER() OVER (ORDER BY COURSE_NAME, COURSE,
-- COURSE_TYPE, COURSE_VERSION, LANGUAGE, VENDOR) COURSE_KEY
-- FROM COURSE