{{ config(materialized='table') }}

select

    r.review_id,

    dc.customer_sk,

    dp.product_sk,

    r.rating,

    r.review_date

from {{ ref('stg_reviews') }} r

left join {{ ref('dim_customer') }} dc
    on r.customer_id = dc.customer_id
   and r.review_date >= dc.dbt_valid_from
   and r.review_date < coalesce(dc.dbt_valid_to, '9999-12-31')

left join {{ ref('dim_product') }} dp
    on r.product_id = dp.product_id
   and r.review_date >= dp.dbt_valid_from
   and r.review_date < coalesce(dp.dbt_valid_to, '9999-12-31')