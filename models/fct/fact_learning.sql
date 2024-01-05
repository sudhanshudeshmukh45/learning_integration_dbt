{{
    config(
        materialized='incremental',
        unique_key = 'enrollment_id',
        merge_exclude_columns=['CREATED_AT']
    )
}}

WITH fact AS (
  SELECT *
  FROM {{ref("src_combined")}}
)

SELECT
  f.ENROLLMENT_ID,
  r.rating_key,
  loc.LOCATION_KEY,
  seg.organization_key,
  b.business_key,
  ass.ASSIGNMENT_KEY,
  c.COURSE_KEY,
  j.JOB_KEY,
  e.ENROLLMENT_KEY,
  NVL(TO_NUMBER(TO_CHAR(CERTIFICATE_DATE, 'YYYYMMDD')),-999) AS CERTIFICATE_DATE_KEY,
  NVL(TO_NUMBER(TO_CHAR(DATE_COMPLETED,'YYYYMMDD')),-999) AS DATE_COMPLETED_KEY,
  f.EMPLOYEE_NUMBER,
  NVL(TO_NUMBER(TO_CHAR(DATE_EDITED, 'YYYYMMDD')),-999) AS DATE_EDITED_KEY,
  NVL(TO_NUMBER(TO_CHAR(DATE_ENROLLED, 'YYYYMMDD')),-999) AS DATE_ENROLLED_KEY,
  NVL(TO_NUMBER(TO_CHAR(DATE_EXPIRES, 'YYYYMMDD')),-999) AS DATE_EXPIRES_KEY,
  NVL(TO_NUMBER(TO_CHAR(LAST_LOGGEDIN, 'YYYYMMDD')),-999)AS LAST_LOGGEDIN_KEY,
  f.Salary,
  f.Currency_Code,
  NVL(TO_NUMBER(TO_CHAR(Original_Hire_Date, 'YYYYMMDD')),-999) AS Original_Hire_Date_KEY,
  NVL(TO_NUMBER(TO_CHAR(Latest_Hire_Date, 'YYYYMMDD')),-999) AS Latest_Hire_Date_KEY,
  NVL(TO_NUMBER(TO_CHAR(Adjusted_Service_Date, 'YYYYMMDD')),-999) AS Adjusted_Service_Date_KEY,
  f.Length_Of_Service,
  f.Headcount,
  f.PROGRESS,
  f.SCORE,
  f.STATUS,
  f.TIMESPENT_MIN,
  f.FTE,
  f.Manager_FTV_ID,
  CURRENT_TIMESTAMP() AS CREATED_AT,
  CURRENT_TIMESTAMP() AS UPDATED_AT
FROM fact f
LEFT OUTER JOIN {{ref("dim_rating")}} r ON f.Performance_Rating = r.Performance_Rating
                              AND f.potential_rating = r.potential_rating

LEFT OUTER JOIN {{ref("dim_location")}} loc ON f.WORK_location = loc.WORK_location
                                 AND f.WORK_city = loc.WORK_city
                                 AND f.WORK_state = loc.WORK_state
                                 AND f.WORK_country = loc.WORK_country
                                 AND f.WORK_region = loc.WORK_region

LEFT OUTER JOIN {{ref("dim_job")}} j ON f.JOB_TITLE = j.JOB_TITLE
                         AND f.JOB_FUNCTION = j.JOB_FUNCTION
                         AND f.JOB_FAMILY = j.JOB_FAMILY
                         AND f.EEO_CATEGORY = j.EEO_CATEGORY
                         AND f.CAREER_BAND = j.CAREER_BAND
                         AND f.CAREER_LEVEL = j.CAREER_LEVEL

LEFT OUTER JOIN {{ref("dim_assignment")}} ass ON f.ASSIGNMENT_NAME = ass.ASSIGNMENT_NAME
                                   AND f.ASSIGNMENT_EMPLOYMENT_CATEGORY = ass.ASSIGNMENT_EMPLOYMENT_CATEGORY
                                   AND f.ASSIGNMENT_CURRENT_CHG_ACTION = ass.ASSIGNMENT_CURRENT_CHG_ACTION
                                   AND f.ASSIGNMENT_CURRENT_CHG_REASON = ass.ASSIGNMENT_CURRENT_CHG_REASON
                                   AND f.ASSIGNMENT_STATUS = ass.ASSIGNMENT_STATUS
                                   AND f.HOURLY_SALARIED = ass.HOURLY_SALARIED
                                   AND f.GRADE_NAME = ass.GRADE_NAME
                                   AND f.ASSIGNMENT_CURRENT_EFFECTIVE_DATE=ass.ASSIGNMENT_CURRENT_EFFECTIVE_DATE

LEFT OUTER JOIN {{ref("dim_organization")}} seg ON f.segment = seg.segment
                                    AND f.opco = seg.opco
                                    AND f.sub_opco = seg.sub_opco
                                    AND f.department_name = seg.department_name

LEFT OUTER JOIN {{ref("dim_business_entity")}} b ON f.business_unit_name = b.business_unit_name
                                      AND f.legal_entity_name = b.legal_entity_name

LEFT OUTER JOIN {{ref("dim_course")}} c ON f.course_name=c.course_name 
and f.course = c.course 
and f.course_type=c.course_type 
and f.course_version=c.course_version 
and f.language=c.language and f.vendor=c.vendor

LEFT OUTER JOIN {{ref("dim_enrollment")}} e on f.IS_ENROLLED=e.IS_ENROLLED 
and f.ENROLLMENT_METHOD=e.ENROLLMENT_METHOD 
and f.ATTAINED_CERTIFICATE=e.ATTAINED_CERTIFICATE
and f.IS_ACTIVE = e.IS_ACTIVE
and f.IS_DELETED= e.IS_DELETED

