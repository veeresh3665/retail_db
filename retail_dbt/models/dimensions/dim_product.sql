{{ config(materialized='table') }}

select

    {{ dbt_utils.generate_surrogate_key(['p.product_id']) }} AS product_sk,

    p.product_id,

    p.product_name,

    c.category_name,

    s.supplier_name,

    {{ null_handler('p.brand') }} AS brand,

    p.cost_price,

    p.selling_price,

    p.weight,

    p.dbt_valid_from,

    p.dbt_valid_to,

    case
        when p.dbt_valid_to is null then 'Current'
        else 'Expired'
    end as product_status,

    {{ audit_columns() }}

from {{ ref('product_snapshot') }} p

left join {{ ref('stg_categories') }} c
    on p.category_id = c.category_id

left join {{ ref('stg_suppliers') }} s
    on p.supplier_id = s.supplier_id