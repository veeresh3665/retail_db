{{ config(
    materialized='incremental',
    unique_key='inventory_id',
    tags=['inventory']
) }}

select

    i.inventory_id,

    ds.store_sk,

    dp.product_sk,

    i.stock,

    i.reorder_level,

    i.last_updated

from {{ ref('stg_inventory') }} i

left join {{ ref('dim_store') }} ds
    on i.store_id = ds.store_id

left join {{ ref('dim_product') }} dp
    on i.product_id = dp.product_id

{% if is_incremental() %}

where i.last_updated >
(
    select max(last_updated)
    from {{ this }}
)

{% endif %}