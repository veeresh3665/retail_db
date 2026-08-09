{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['supplier_id']) }} AS supplier_sk,
    supplier_id,
    supplier_name,
    country,
    email,
    phone

from {{ ref('stg_suppliers') }}