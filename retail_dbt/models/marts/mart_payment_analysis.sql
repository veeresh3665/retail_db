{{ config(materialized='table') }}

select

    payment_method,

    payment_status,

    count(*) as total_transactions,

    sum(amount) as total_amount

from {{ ref('fact_payments') }}

group by

    payment_method,
    payment_status