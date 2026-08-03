{{ config(materialized='view') }}

select

    trim(product_id)                      as product_id,
    trim(product_name)                    as product_name,
    trim(category_id)                     as category_id,
    trim(supplier_id)                     as supplier_id,
    trim(brand)                           as brand,

    cast(cost_price as number(10,2))      as cost_price,
    cast(selling_price as number(10,2))   as selling_price,
    cast(weight as number(10,2))          as weight

from {{ source('raw', 'products') }}