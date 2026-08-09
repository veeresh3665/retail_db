select

p.product_name,

sum(f.quantity) total_quantity,

sum(f.line_total) total_sales

from {{ ref('fact_sales') }} f

join {{ ref('dim_product') }} p

on f.product_id=p.product_id

group by

p.product_name

order by

total_sales desc

limit 10;