
{{
    config(
        materialized='incremental',
        unique_key = 'enrollment_id',
        merge_exclude_columns =  ['CREATED_AT'],
        full_refresh = False
    )
}}

WITH raw_learning AS (
        SELECT * FROM {{source('SUDHANSHU','INCREMENTAL')}}
)

SELECT
    NVL(ATTAINED_CERTIFICATE,'NA') AS ATTAINED_CERTIFICATE,
    CERTIFICATE_DATE,
    COURSE,
    COURSE_NAME,
    COURSE_TYPE,
    NVL(COURSE_VERSION,'NA') AS COURSE_VERSION,
    DATE_COMPLETED,
    DATE_EDITED,
    DATE_ENROLLED,
    DATE_EXPIRES,
    DATE_HIRED,
    DATE_STARTED,
    EMPLOYEE_NUMBER,
    ENROLLMENT_ID,
    NVL(ENROLLMENT_METHOD,'NA') AS ENROLLMENT_METHOD,
    NVL(IS_ENROLLED,'NA') AS IS_ENROLLED,
    LANGUAGE,
    LAST_LOGGEDIN,
    PROGRESS,
    SCORE,
    CASE
        WHEN STATUS = 'NotComplete' THEN 'Not Complete'
        WHEN STATUS = 'NotStarted' THEN 'Not Started'
        WHEN STATUS = 'InProgress' THEN 'In Progress'
        WHEN STATUS = 'PendingApproval' THEN 'Pending Approval'
        WHEN STATUS = 'PendingEvaluationRequired' THEN 'Pending Evaluation Required'
        WHEN STATUS = 'NotApplicable' THEN 'Not Applicable'
        WHEN STATUS = 'N/A' THEN 'Not Applicable'
        ELSE STATUS
    END AS STATUS,
    TIMESPENT_MIN,
    NVL(VENDOR,'NA') AS VENDOR,
    nvl(IS_ACTIVE,'NA') AS IS_ACTIVE,
    CASE
        WHEN IS_DELETED IS NULL THEN 'NA'
        WHEN IS_DELETED = 'TRUE' THEN 'True'
        WHEN IS_DELETED = 'FALSE' THEN 'False'
    ELSE IS_DELETED
    END AS IS_DELETED,
    CURRENT_TIMESTAMP() AS CREATED_AT,
    CURRENT_TIMESTAMP() AS UPDATED_AT
FROM
    raw_learning


