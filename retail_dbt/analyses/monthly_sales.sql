select

date_trunc('month',order_date) as month,

sum(line_total) as total_sales,

count(distinct order_id) as total_orders

from {{ ref('fact_sales') }}

group by 1

order by 1;