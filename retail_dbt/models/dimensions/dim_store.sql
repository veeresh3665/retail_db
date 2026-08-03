{{ config(materialized='table') }}

select

    store_id,
    store_name,
    city,
    state,
    country

from {{ ref('stg_stores') }}