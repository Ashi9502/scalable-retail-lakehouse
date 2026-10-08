select
    cast(product_id as bigint) as product_id,
    cast(product_name as varchar) as product_name,
    cast(category as varchar) as category,
    cast(unit_price as double) as unit_price,
    cast(updated_at as timestamp) as updated_at
from read_parquet('{{ var("silver_path") }}/products/*.parquet')
