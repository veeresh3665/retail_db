select

order_date,

sum(line_total) sales

from {{ ref('fact_sales') }}

group by

order_date

order by

order_date;