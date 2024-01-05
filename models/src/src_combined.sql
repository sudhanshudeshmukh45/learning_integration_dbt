
{{
    config(
        materialized='incremental',
        unique_key='enrollment_id',
        merge_exclude_columns=['CREATED_AT']
    )
}}

WITH ACTIVE_EMPLOYEES_CLEANSED AS (
    SELECT * FROM {{ref("src_active_employees")}}
),
LEARNING_CLEANSED AS (
    SELECT * FROM {{ref("src_learning")}}
)

select * from LEARNING_CLEANSED L inner join ACTIVE_EMPLOYEES_CLEANSED A on A.ftv_id=L.EMPLOYEE_NUMBER

