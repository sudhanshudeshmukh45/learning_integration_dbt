
{{
    config(
        materialized='incremental',
        pre_hook="TRUNCATE TABLE ACTIVE.src_combined;"

    )
}}

WITH last_time_run AS (
    SELECT LAST_UPDATED_DATE AS last_run_date
    FROM {{ref('control')}}
),
ACTIVE_EMPLOYEES_CLEANSED AS (
    SELECT * FROM {{ref("src_active_employees")}}
),
LEARNING_CLEANSED AS (
    SELECT * FROM {{ref('src_learning_combined')}}

    {% if is_incremental() %}
        WHERE updated_at>(SELECT last_run_date from last_time_run)
    {% endif %}

)

select * from LEARNING_CLEANSED L inner join ACTIVE_EMPLOYEES_CLEANSED A on A.ftv_id=L.EMPLOYEE_NUMBER

