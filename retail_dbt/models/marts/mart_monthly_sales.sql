{{ config(materialized='table') }}

select

    d.year,
    d.month,
    d.month_name,

    sum(f.line_total) as total_sales,

    count(distinct f.order_id) as total_orders,

    sum(f.quantity) as total_quantity

from {{ ref('fact_sales') }} f

join {{ ref('dim_date') }} d
    on f.order_date = d.calendar_date

group by
    d.year,
    d.month,
    d.month_name

order by
    d.year,
    d.month