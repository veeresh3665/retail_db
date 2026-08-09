{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['category_id']) }} AS category_sk,
    category_id,
    category_name

from {{ ref('stg_categories') }}