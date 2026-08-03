{{ config(materialized='table') }}

select

    p.product_id,
    p.product_name,

    c.category_name,

    s.supplier_name,

    p.brand,

    p.cost_price,

    p.selling_price,

    p.weight

from {{ ref('stg_products') }} p

left join {{ ref('stg_categories') }} c
on p.category_id = c.category_id

left join {{ ref('stg_suppliers') }} s
on p.supplier_id = s.supplier_id