select

    order_item_id,

    count(*) as cnt

from {{ ref('fact_sales') }}

group by

    order_item_id

having count(*) > 1