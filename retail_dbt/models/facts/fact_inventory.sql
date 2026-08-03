{{ config(materialized='table') }}

select

    inventory_id,

    store_id,

    product_id,

    stock,

    reorder_level,

    last_updated

from {{ ref('stg_inventory') }}