select *

from {{ ref('dim_customer') }}

where email not like '%@%'