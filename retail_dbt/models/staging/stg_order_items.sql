{{ config(materialized='view') }}

select

    trim(order_item_id)              as order_item_id,

    trim(order_id)                   as order_id,

    trim(product_id)                 as product_id,

    cast(quantity as integer)        as quantity,

    cast(unit_price as number(12,2)) as unit_price,

    cast(discount as number(5,2))    as discount,

    cast(line_total as number(12,2)) as line_total,

    _FIVETRAN_SYNCED

from {{ source('raw', 'order_items') }}