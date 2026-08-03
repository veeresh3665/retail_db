{{ config(materialized='table') }}

select

    employee_id,
    store_id,
    employee_name,
    department,
    salary,
    joining_date

from {{ ref('stg_employees') }}