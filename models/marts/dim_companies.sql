{{ config(materialized='table', schema='marts') }}

WITH companies_grouped AS (
    SELECT 
        COMPANY,
        -- Take the first non-null value for each field
        FIRST_VALUE(COMPANY_PUBLIC_RESPONSE) OVER (PARTITION BY COMPANY ORDER BY CASE WHEN COMPANY_PUBLIC_RESPONSE IS NOT NULL THEN 0 ELSE 1 END) AS company_public_response,
        FIRST_VALUE(COMPANY_RESPONSE_TO_CONSUMER) OVER (PARTITION BY COMPANY ORDER BY CASE WHEN COMPANY_RESPONSE_TO_CONSUMER IS NOT NULL THEN 0 ELSE 1 END) AS company_response_to_consumer,
        FIRST_VALUE(TIMELY_RESPONSE) OVER (PARTITION BY COMPANY ORDER BY CASE WHEN TIMELY_RESPONSE IS NOT NULL THEN 0 ELSE 1 END) AS timely_response,
        FIRST_VALUE(CONSUMER_DISPUTED) OVER (PARTITION BY COMPANY ORDER BY CASE WHEN CONSUMER_DISPUTED IS NOT NULL THEN 0 ELSE 1 END) AS consumer_disputed
    FROM {{ ref('stg__consumer_complaints') }}
    WHERE COMPANY IS NOT NULL
),

companies AS (
    SELECT DISTINCT
        COMPANY,
        company_public_response AS COMPANY_PUBLIC_RESPONSE,
        company_response_to_consumer AS COMPANY_RESPONSE_TO_CONSUMER,
        timely_response AS TIMELY_RESPONSE,
        consumer_disputed AS CONSUMER_DISPUTED
    FROM companies_grouped
),

companies_with_keys AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['COMPANY']) }} AS company_key,
        COMPANY AS company_name,
        COMPANY_PUBLIC_RESPONSE AS company_public_response,
        COMPANY_RESPONSE_TO_CONSUMER AS company_response_to_consumer,
        CASE 
            WHEN TIMELY_RESPONSE = 'Yes' THEN TRUE
            WHEN TIMELY_RESPONSE = 'No' THEN FALSE
            ELSE NULL
        END AS timely_response_flag,
        CASE 
            WHEN CONSUMER_DISPUTED = 'Yes' THEN TRUE
            WHEN CONSUMER_DISPUTED = 'No' THEN FALSE
            ELSE NULL
        END AS consumer_disputed_flag,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM companies
)

SELECT * FROM companies_with_keys
