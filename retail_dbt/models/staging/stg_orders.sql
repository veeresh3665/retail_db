{{ config(materialized='view') }}

select

    trim(order_id)                     as order_id,

    trim(customer_id)                  as customer_id,

    trim(store_id)                     as store_id,

    cast(order_date as date)           as order_date,

    upper(trim(order_status))          as order_status,

    cast(total_amount as number(12,2))  as total_amount,

    _FIVETRAN_SYNCED

from {{ source('raw', 'orders') }}

where order_date >= '{{ var("historical_load_date") }}'