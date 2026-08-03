{{ config(materialized='view') }}

select

    trim(store_id) as store_id,

    trim(store_name) as store_name,

    trim(city) as city,

    trim(state) as state,

    trim(country) as country

from {{ source('raw','stores') }}