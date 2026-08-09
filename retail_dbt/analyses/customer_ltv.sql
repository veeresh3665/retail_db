select

customer_id,

sum(line_total) lifetime_value

from {{ ref('fact_sales') }}

group by customer_id

order by lifetime_value desc;