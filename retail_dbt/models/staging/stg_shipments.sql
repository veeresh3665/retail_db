{{ config(materialized='view') }}

select

    trim(shipment_id) as shipment_id,

    trim(order_id) as order_id,

    trim(courier) as courier,

    cast(shipment_date as date) as shipment_date,

    cast(delivery_date as date) as delivery_date,

    upper(trim(delivery_status)) as delivery_status

from {{ source('raw','shipments') }}