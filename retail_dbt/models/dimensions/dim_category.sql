{{ config(materialized='table') }}

select

    category_id,
    category_name

from {{ ref('stg_categories') }}