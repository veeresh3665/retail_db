{{ config(materialized='table') }}

select

    review_id,

    customer_id,

    product_id,

    rating,

    review_date

from {{ ref('stg_reviews') }}

