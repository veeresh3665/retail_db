{{ config(materialized='table') }}

with dates as (

    select dateadd(day, seq4(), '2025-01-01') as calendar_date

    from table(generator(rowcount => 365))

)

select

    calendar_date,

    year(calendar_date) as year,

    month(calendar_date) as month,

    monthname(calendar_date) as month_name,

    quarter(calendar_date) as quarter,

    day(calendar_date) as day,

    dayname(calendar_date) as day_name,

    week(calendar_date) as week

from dates