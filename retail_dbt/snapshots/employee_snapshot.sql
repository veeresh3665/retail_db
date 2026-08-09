{% snapshot employee_snapshot %}

{{
    config(
        target_schema='SNAPSHOT',
        unique_key='employee_id',
        strategy='check',
        check_cols=[
            'employee_name',
            'salary',
            'department'
        ]
    )
}}

select
    employee_id,
    store_id,
    employee_name,
    department,
    salary,
    joining_date
from {{ source('raw', 'employees') }}

{% endsnapshot %}