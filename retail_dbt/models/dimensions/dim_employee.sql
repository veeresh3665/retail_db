{{ config(materialized='table') }}

select

   {{ dbt_utils.generate_surrogate_key(['employee_id']) }} as employee_sk,

    employee_id,

    store_id,

    employee_name,

    department,

    salary,

    joining_date,

    dbt_valid_from,

    dbt_valid_to,

    case
        when dbt_valid_to is null then 'Current'
        else 'Expired'
    end as employee_status,

    {{ audit_columns() }}

from {{ ref('employee_snapshot') }}