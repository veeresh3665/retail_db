{{ config(materialized='table') }}

select

    p.product_id,

    p.product_name,

    p.category_name,

    sum(f.quantity) as quantity_sold,

    sum(f.line_total) as revenue

from {{ ref('fact_sales') }} f

join {{ ref('dim_product') }} p
    on f.product_sk = p.product_sk

group by

    p.product_id,
    p.product_name,
    p.category_name

order by revenue desc