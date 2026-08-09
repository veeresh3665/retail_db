{% snapshot customer_snapshot %}

{{
    config(
        target_schema='SNAPSHOT',
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'customer_name',
            'email',
            'phone',
            'city',
            'state'
        ]
    )
}}

select *

from {{ source('raw','customers') }}

{% endsnapshot %}