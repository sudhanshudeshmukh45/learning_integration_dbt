{{
    config(
        materialized= 'incremental',
        unique_key='business_key'
    )
}}


select
  distinct
  {{ dbt_utils.generate_surrogate_key(['BUSINESS_UNIT_NAME','LEGAL_ENTITY_NAME']) }} as BUSINESS_KEY,
 BUSINESS_UNIT_NAME,LEGAL_ENTITY_NAME
from {{ ref('src_combined') }}




-- WITH BUSINESS_ENTITY AS (
-- SELECT distinct  BUSINESS_UNIT_NAME,LEGAL_ENTITY_NAME
-- FROM
-- {{ref('src_combined')}}
-- )
-- SELECT *,ROW_NUMBER() OVER (ORDER BY BUSINESS_UNIT_NAME,LEGAL_ENTITY_NAME) BUSINESS_KEY
--  FROM BUSINESS_ENTITY