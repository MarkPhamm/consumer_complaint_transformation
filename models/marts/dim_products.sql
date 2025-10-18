{{ config(materialized='table', schema='marts') }}

WITH products AS (
    SELECT DISTINCT
        PRODUCT,
        SUB_PRODUCT
    FROM {{ ref('stg__consumer_complaints') }}
    WHERE PRODUCT IS NOT NULL
),

products_with_keys AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['PRODUCT', 'SUB_PRODUCT']) }} AS product_key,
        PRODUCT AS product_name,
        SUB_PRODUCT AS sub_product_name,
        CASE 
            WHEN SUB_PRODUCT IS NOT NULL THEN PRODUCT || ' - ' || SUB_PRODUCT
            ELSE PRODUCT
        END AS product_full_name,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM products
)

SELECT * FROM products_with_keys