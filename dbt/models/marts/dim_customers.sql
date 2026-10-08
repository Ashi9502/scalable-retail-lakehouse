with ranked as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by updated_at desc
        ) as rn
    from {{ ref('stg_customers') }}
)
select
    customer_id,
    customer_name,
    city,
    signup_date,
    updated_at
from ranked
where rn = 1
