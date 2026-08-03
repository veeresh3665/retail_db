{{ config(materialized='view') }}

select

    trim(employee_id) as employee_id,

    trim(store_id) as store_id,

    trim(employee_name) as employee_name,

    trim(department) as department,

    cast(salary as number(12,2)) as salary,

    cast(joining_date as date) as joining_date

from {{ source('raw','employees') }}