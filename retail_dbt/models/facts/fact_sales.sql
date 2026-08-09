{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    incremental_strategy='merge',
    tags=['sales']
) }}

select

    oi.order_item_id,

    o.order_id,

    dc.customer_sk,

    ds.store_sk,

    dp.product_sk,

    o.order_date,

    oi.quantity,

    {{ amount_format('oi.unit_price') }} as unit_price,

    oi.discount,

    {{ amount_format('oi.line_total') }} as line_total,

    o.order_status,

    greatest(
        o._FIVETRAN_SYNCED,
        oi._FIVETRAN_SYNCED
    ) as _FIVETRAN_SYNCED

from {{ ref('stg_orders') }} o

inner join {{ ref('stg_order_items') }} oi
    on o.order_id = oi.order_id

left join {{ ref('dim_customer') }} dc
    on o.customer_id = dc.customer_id

left join {{ ref('dim_product') }} dp
    on oi.product_id = dp.product_id

left join {{ ref('dim_store') }} ds
    on o.store_id = ds.store_id

{% if is_incremental() %}

where greatest(
        o._FIVETRAN_SYNCED,
        oi._FIVETRAN_SYNCED
      ) >
(
    select coalesce(max(_FIVETRAN_SYNCED), '1900-01-01'::timestamp)
    from {{ this }}
)

{% endif %}