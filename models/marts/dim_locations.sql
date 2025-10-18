{{ config(materialized='table', schema='marts') }}

WITH locations AS (
    SELECT DISTINCT
        STATE,
        ZIP_CODE
    FROM {{ ref('stg__consumer_complaints') }}
    WHERE STATE IS NOT NULL OR ZIP_CODE IS NOT NULL
),

locations_with_keys AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['STATE', 'ZIP_CODE']) }} AS location_key,
        STATE AS state_code,
        ZIP_CODE AS zip_code,
        CASE 
            WHEN STATE IS NOT NULL AND ZIP_CODE IS NOT NULL THEN STATE || ' - ' || ZIP_CODE
            WHEN STATE IS NOT NULL THEN STATE
            WHEN ZIP_CODE IS NOT NULL THEN ZIP_CODE
            ELSE 'Unknown'
        END AS location_description,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM locations
)

SELECT * FROM locations_with_keys
