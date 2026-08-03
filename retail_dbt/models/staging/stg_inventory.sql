{{ config(materialized='view') }}

select

    trim(inventory_id) as inventory_id,

    trim(store_id) as store_id,

    trim(product_id) as product_id,

    cast(stock as integer) as stock,

    cast(reorder_level as integer) as reorder_level,

    cast(last_updated as date) as last_updated

from {{ source('raw','inventory') }}