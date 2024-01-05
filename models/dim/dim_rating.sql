{{
    config(
        materialized= 'incremental',
        unique_key = 'rating_key'
    )
}}

select
  distinct
  {{ dbt_utils.generate_surrogate_key(['POTENTIAL_RATING','PERFORMANCE_RATING']) }} as RATING_KEY,
  POTENTIAL_RATING,
  PERFORMANCE_RATING
from {{ ref('src_combined') }}






-- WITH RATING AS (
-- SELECT distinct POTENTIAL_RATING,PERFORMANCE_RATING
-- FROM
-- {{ref('src_combined')}}
-- WHERE performance_rating is not null and potential_rating is not null
-- )
-- SELECT *,ROW_NUMBER() OVER (ORDER BY POTENTIAL_RATING,PERFORMANCE_RATING) RATING_KEY
--  FROM RATING
 