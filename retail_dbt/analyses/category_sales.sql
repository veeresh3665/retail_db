select

c.category_name,

sum(f.line_total) revenue

from {{ ref('fact_sales') }} f

join {{ ref('dim_product') }} p

on f.product_id=p.product_id

join {{ ref('dim_category') }} c

on p.category_id=c.category_id

group by

c.category_name

order by

revenue desc;