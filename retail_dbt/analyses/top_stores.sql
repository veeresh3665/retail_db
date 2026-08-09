select

s.store_name,

sum(f.line_total) total_sales

from {{ ref('fact_sales') }} f

join {{ ref('dim_store') }} s

on f.store_id=s.store_id

group by

s.store_name

order by

total_sales desc;