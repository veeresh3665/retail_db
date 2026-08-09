{{ config(
    materialized='incremental',
    unique_key='payment_id'
) }}

select

    payment_id,

    order_id,

    payment_method,

    payment_status,

    amount,

    payment_date

from {{ ref('stg_payments') }}

{% if is_incremental() %}

where payment_date >
(
    select max(payment_date)
    from {{ this }}
)

{% endif %}