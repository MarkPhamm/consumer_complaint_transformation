{{ config(materialized='table', schema='marts') }}

WITH complaint_dates AS (
    SELECT DISTINCT
        DATE_RECEIVED,
        DATE_SENT_TO_COMPANY
    FROM {{ ref('stg__consumer_complaints') }}
    WHERE DATE_RECEIVED IS NOT NULL OR DATE_SENT_TO_COMPANY IS NOT NULL
),

all_dates AS (
    SELECT DATE_RECEIVED AS complaint_date FROM complaint_dates WHERE DATE_RECEIVED IS NOT NULL
    UNION
    SELECT DATE_SENT_TO_COMPANY AS complaint_date FROM complaint_dates WHERE DATE_SENT_TO_COMPANY IS NOT NULL
),

dates_with_keys AS (
    SELECT DISTINCT
        complaint_date,
        {{ dbt_utils.generate_surrogate_key(['complaint_date']) }} AS date_key,
        EXTRACT(YEAR FROM complaint_date) AS year,
        EXTRACT(MONTH FROM complaint_date) AS month,
        EXTRACT(DAY FROM complaint_date) AS day,
        EXTRACT(QUARTER FROM complaint_date) AS quarter,
        EXTRACT(DAYOFWEEK FROM complaint_date) AS day_of_week,
        EXTRACT(DAYOFYEAR FROM complaint_date) AS day_of_year,
        DATE_TRUNC('month', complaint_date) AS month_start,
        DATE_TRUNC('quarter', complaint_date) AS quarter_start,
        DATE_TRUNC('year', complaint_date) AS year_start,
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM complaint_date) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        CASE 
            WHEN EXTRACT(MONTH FROM complaint_date) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM complaint_date) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM complaint_date) IN (6, 7, 8) THEN 'Summer'
            WHEN EXTRACT(MONTH FROM complaint_date) IN (9, 10, 11) THEN 'Fall'
        END AS season,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM all_dates
)

SELECT * FROM dates_with_keys
