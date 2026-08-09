select *

from {{ ref('fact_inventory') }}

where stock < 0