select
  event_id,
  event_date,
  user_id,
  anonymous_id,
  JSONExtractString(properties, 'product_id') as product_id,
  JSONExtractString(properties, 'sku_id') as sku_id,
  JSONExtractInt(properties, 'quantity') as quantity,
  JSONExtractFloat(properties, 'price') as price,
  JSONExtractString(properties, 'currency') as currency,
  quantity * price as gmv,
  event_time
from {{ ref('dwd_app_behavior_events') }}
where event_name = 'add_to_cart'
