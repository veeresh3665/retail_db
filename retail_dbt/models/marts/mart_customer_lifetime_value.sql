{{ config(materialized='table') }}

select

    c.customer_id,

    c.customer_name,

    count(distinct f.order_id) as total_orders,

    sum(f.line_total) as lifetime_value

from {{ ref('fact_sales') }} f

join {{ ref('dim_customer') }} c
    on f.customer_sk = c.customer_sk

group by

    c.customer_id,
    c.customer_name

order by lifetime_value desc