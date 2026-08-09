{% snapshot product_snapshot %}

{{
    config(
        target_schema='SNAPSHOT',
        unique_key='product_id',
        strategy='check',
        check_cols=[
            'product_name',
            'brand',
            'selling_price',
            'cost_price'
        ]
    )
}}

select *

from {{ source('raw','products') }}

{% endsnapshot %}