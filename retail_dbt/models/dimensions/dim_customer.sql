{{ config(materialized='table') }}

select

    customer_id,
    customer_name,
    gender,
    dob,
    email,
    phone,
    city,
    state,
    country,
    join_date

from {{ ref('stg_customers') }}