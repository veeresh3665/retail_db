{{ config(materialized='view') }}

select

    trim(supplier_id) as supplier_id,

    trim(supplier_name) as supplier_name,

    trim(country) as country,

    lower(trim(email)) as email,

    trim(phone) as phone

from {{ source('raw','suppliers') }}