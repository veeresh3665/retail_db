{{ config(materialized='view') }}

select

    trim(review_id) as review_id,

    trim(customer_id) as customer_id,

    trim(product_id) as product_id,

    cast(rating as integer) as rating,

    trim(review_text) as review_text,

    cast(review_date as date) as review_date

from {{ source('raw','reviews') }}