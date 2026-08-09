{{ config(materialized='table') }}

select

    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,

    customer_id,

    customer_name,

    gender,

    dob,

    email,

    phone,

    city,

    state,

    country,

    join_date,

    dbt_valid_from,

    dbt_valid_to,

    case
        when dbt_valid_to is null then 'Current'
        else 'Expired'
    end as customer_status,

    {{ audit_columns() }}

from {{ ref('customer_snapshot') }}