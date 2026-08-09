select

store_name,

product_name,

stock,

reorder_level,

inventory_status

from {{ ref('mart_inventory_summary') }}

order by stock;