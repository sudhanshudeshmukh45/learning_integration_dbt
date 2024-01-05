{{
    config(
        materialized= 'table'
    )
}}

select
  distinct
  {{ dbt_utils.generate_surrogate_key(['SEGMENT','OPCO','SUB_OPCO','DEPARTMENT_NAME']) }} as ORGANIZATION_KEY,
  SEGMENT,OPCO,SUB_OPCO,DEPARTMENT_NAME
from {{ ref('src_active_employees') }}


-- WITH ORGANIZATION AS (
-- SELECT distinct SEGMENT,OPCO,SUB_OPCO,DEPARTMENT_NAME
-- FROM
-- {{ref('src_combined')}}
-- )
-- SELECT *,ROW_NUMBER() OVER (ORDER BY SEGMENT,OPCO,SUB_OPCO,DEPARTMENT_NAME) ORGANIZATION_KEY
-- FROM ORGANIZATION



