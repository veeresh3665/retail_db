{{ config(materialized='table') }}

select

    oi.order_item_id,

    o.order_id,

    o.customer_id,

    o.store_id,

    oi.product_id,

    o.order_date,

    oi.quantity,

    oi.unit_price,

    oi.discount,

    oi.line_total,

    o.order_status

from {{ ref('stg_orders') }} o

inner join {{ ref('stg_order_items') }} oi
    on o.order_id = oi.order_id