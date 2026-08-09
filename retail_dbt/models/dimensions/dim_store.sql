{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['store_id']) }} AS store_sk,
    store_id,

    store_name,

    city,

    state,

    '{{ var("country_name") }}' as country

from {{ ref('stg_stores') }}