select
    cast(customer_id as bigint) as customer_id,
    cast(customer_name as varchar) as customer_name,
    cast(city as varchar) as city,
    cast(signup_date as date) as signup_date,
    cast(updated_at as timestamp) as updated_at
from read_parquet('{{ var("silver_path") }}/customers/*.parquet')
