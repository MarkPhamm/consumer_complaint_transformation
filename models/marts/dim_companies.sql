{{ config(materialized='table', schema='marts') }}

WITH companies AS (
    SELECT DISTINCT
        COMPANY,
        COMPANY_PUBLIC_RESPONSE,
        COMPANY_RESPONSE_TO_CONSUMER,
        TIMELY_RESPONSE,
        CONSUMER_DISPUTED
    FROM {{ ref('stg__consumer_complaints') }}
    WHERE COMPANY IS NOT NULL
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
