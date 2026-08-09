{{ config(materialized='view') }}

select

    trim(customer_id)                     as customer_id,
    {{ clean_text('CUSTOMER_NAME') }} AS CUSTOMER_NAME,
    trim(gender)                          as gender,

    cast(dob as date)                     as dob,

   {{ clean_text('EMAIL') }} AS EMAIL,

    trim(phone)                           as phone,

    trim(city)                            as city,
    trim(state)                           as state,
    trim(country)                         as country,

    cast(join_date as date)               as join_date

from {{ source('raw', 'customers') }}