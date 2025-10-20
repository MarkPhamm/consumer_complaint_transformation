{{
    config(
        materialized='table',
        schema='marts',
        static_analysis='unsafe'
    )
}}

{{ dbt_date.get_base_dates(start_date="2020-01-01", end_date="2025-12-31") }}
