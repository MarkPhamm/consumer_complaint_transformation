{{
    config(
        materialized='table',
        schema='marts'
    )
}}

SELECT
    DATE_TRUNC('day', date_day) AS date_day
FROM {{ ref('time_spine') }}

