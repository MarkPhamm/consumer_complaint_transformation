{{ config(materialized='table', schema='marts') }}

WITH complaints AS (
    SELECT
        COMPLAINT_ID,
        DATE_RECEIVED,
        DATE_SENT_TO_COMPANY,
        PRODUCT,
        SUB_PRODUCT,
        ISSUE,
        SUB_ISSUE,
        COMPANY,
        STATE,
        ZIP_CODE,
        TAGS,
        CONSUMER_CONSENT_PROVIDED,
        SUBMITTED_VIA,
        COMPANY_RESPONSE_TO_CONSUMER,
        TIMELY_RESPONSE,
        CONSUMER_DISPUTED,
        COMPLAINT_WHAT_HAPPENED,
        COMPANY_PUBLIC_RESPONSE,
        LOAD_TIMESTAMP
    FROM {{ ref('stg__consumer_complaints') }}
),

complaints_with_keys AS (
    SELECT
        COMPLAINT_ID AS complaint_id,
        
        -- Foreign Keys to Dimensions
        {{ dbt_utils.generate_surrogate_key(['COMPANY']) }} AS company_key,
        {{ dbt_utils.generate_surrogate_key(['PRODUCT', 'SUB_PRODUCT']) }} AS product_key,
        {{ dbt_utils.generate_surrogate_key(['ISSUE', 'SUB_ISSUE']) }} AS issue_key,
        {{ dbt_utils.generate_surrogate_key(['STATE', 'ZIP_CODE']) }} AS location_key,
        {{ dbt_utils.generate_surrogate_key(['DATE_RECEIVED']) }} AS date_received_key,
        {{ dbt_utils.generate_surrogate_key(['DATE_SENT_TO_COMPANY']) }} AS date_sent_key,
        
        -- Date Fields
        DATE_RECEIVED AS date_received,
        DATE_SENT_TO_COMPANY AS date_sent_to_company,
        
        -- Response Time Calculation
        CASE 
            WHEN DATE_RECEIVED IS NOT NULL AND DATE_SENT_TO_COMPANY IS NOT NULL 
            THEN DATEDIFF('day', DATE_RECEIVED, DATE_SENT_TO_COMPANY)
            ELSE NULL
        END AS days_to_send_to_company,
        
        -- Flags
        CASE 
            WHEN CONSUMER_CONSENT_PROVIDED = 'Consent provided' THEN TRUE
            WHEN CONSUMER_CONSENT_PROVIDED = 'Consent not provided' THEN FALSE
            ELSE NULL
        END AS consumer_consent_provided_flag,
        
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
        
        -- Text Fields
        TAGS AS tags,
        SUBMITTED_VIA AS submitted_via,
        COMPLAINT_WHAT_HAPPENED AS complaint_description,
        COMPANY_PUBLIC_RESPONSE AS company_public_response,
        
        -- Metadata
        LOAD_TIMESTAMP AS load_timestamp,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
        
    FROM complaints
)

SELECT * FROM complaints_with_keys
