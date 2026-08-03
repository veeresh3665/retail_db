{{ config(materialized='view') }}

select

    trim(customer_id)                     as customer_id,
    trim(customer_name)                   as customer_name,
    trim(gender)                          as gender,

    cast(dob as date)                     as dob,

    lower(trim(email))                    as email,

    trim(phone)                           as phone,

    trim(city)                            as city,
    trim(state)                           as state,
    trim(country)                         as country,

    cast(join_date as date)               as join_date

from {{ source('raw', 'customers') }}