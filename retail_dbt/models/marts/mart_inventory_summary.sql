{{ config(materialized='table') }}

select

    s.store_name,

    p.product_name,

    i.stock,

    i.reorder_level,

    case
        when i.stock < i.reorder_level
        then 'REORDER'
        else 'SUFFICIENT'
    end as inventory_status

from {{ ref('fact_inventory') }} i

join {{ ref('dim_store') }} s
    on i.store_id = s.store_id

join {{ ref('dim_product') }} p
    on i.product_id = p.product_id