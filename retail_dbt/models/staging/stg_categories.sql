{{ config(materialized='view') }}

select

    trim(category_id) as category_id,

    trim(category_name) as category_name

from {{ source('raw','categories') }}