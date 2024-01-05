
{{
    config(
        materialized = 'table'
    )
}}
 
 
WITH persons AS (
    SELECT * FROM {{ref('src_active_employees')}}
)
 
SELECT
distinct
FTV_ID as EMPLOYEE_NUMBER,
EMAIL_ADDRESS,
PERSON_DISPLAY_NAME,
Person_Type,
Worker_Type,
Date_of_Birth,
Age,
L1_Leader
FROM
persons




