{{ config(materialized='table') }}

select

    supplier_id,
    supplier_name,
    country,
    email,
    phone

from {{ ref('stg_suppliers') }}