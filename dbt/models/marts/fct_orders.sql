select
    o.order_id,
    o.customer_id,
    o.product_id,
    o.order_ts,
    o.order_date,
    o.quantity,
    o.unit_price,
    o.discount_pct,
    o.order_amount,
    o.status,
    o.payment_method
from {{ ref('stg_orders') }} o
inner join {{ ref('dim_customers') }} c using (customer_id)
inner join {{ ref('dim_products') }} p using (product_id)
