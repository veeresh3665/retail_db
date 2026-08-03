{{ config(materialized='view') }}

select

    trim(payment_id) as payment_id,

    trim(order_id) as order_id,

    trim(payment_method) as payment_method,

    upper(trim(payment_status)) as payment_status,

    cast(amount as number(12,2)) as amount,

    cast(payment_date as date) as payment_date

from {{ source('raw','payments') }}