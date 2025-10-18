{{ config(materialized='table', schema='marts') }}

WITH issues AS (
    SELECT DISTINCT
        ISSUE,
        SUB_ISSUE
    FROM {{ ref('stg__consumer_complaints') }}
    WHERE ISSUE IS NOT NULL
),

issues_with_keys AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['ISSUE', 'SUB_ISSUE']) }} AS issue_key,
        ISSUE AS issue_name,
        SUB_ISSUE AS sub_issue_name,
        CASE 
            WHEN SUB_ISSUE IS NOT NULL THEN ISSUE || ' - ' || SUB_ISSUE
            ELSE ISSUE
        END AS issue_full_name,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM issues
)

SELECT * FROM issues_with_keys
