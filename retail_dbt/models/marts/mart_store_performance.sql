{{ config(materialized='table') }}

select

    s.store_name,

    s.city,

    count(distinct f.order_id) as total_orders,

    sum(f.line_total) as total_sales

from {{ ref('fact_sales') }} f

join {{ ref('dim_store') }} s
    on f.store_sk = s.store_sk

group by

    s.store_name,
    s.city

order by total_sales desc