select *

from {{ ref('fact_sales') }}

where

line_total < discount