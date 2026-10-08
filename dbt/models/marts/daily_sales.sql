select
    order_date,
    count(distinct order_id) as orders,
    round(sum(order_amount), 2) as gross_sales,
    round(avg(order_amount), 2) as avg_order_value
from {{ ref('fct_orders') }}
where status <> 'CANCELLED'
group by 1
