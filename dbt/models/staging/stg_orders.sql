select
    cast(order_id as bigint) as order_id,
    cast(customer_id as bigint) as customer_id,
    cast(product_id as bigint) as product_id,
    cast(order_ts as timestamp) as order_ts,
    cast(order_date as date) as order_date,
    cast(quantity as integer) as quantity,
    cast(unit_price as double) as unit_price,
    cast(discount_pct as double) as discount_pct,
    cast(order_amount as double) as order_amount,
    cast(status as varchar) as status,
    cast(payment_method as varchar) as payment_method
from read_parquet('{{ var("silver_path") }}/orders/*.parquet')
