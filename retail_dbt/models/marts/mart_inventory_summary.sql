{{ config(
    materialized='table',
    tags=['sales']
) }}

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
    on i.store_sk = s.store_sk

join {{ ref('dim_product') }} p
    on i.product_sk = p.product_sk