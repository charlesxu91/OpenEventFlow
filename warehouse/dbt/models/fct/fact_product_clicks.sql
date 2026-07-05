select
  event_id,
  event_date,
  user_id,
  anonymous_id,
  JSONExtractString(properties, 'product_id') as product_id,
  JSONExtractString(properties, 'page') as page,
  JSONExtractInt(properties, 'position') as position,
  JSONExtractString(properties, 'click_id') as click_id,
  JSONExtractString(properties, 'recommend_trace_id') as recommend_trace_id,
  event_time
from {{ ref('dwd_app_behavior_events') }}
where event_name = 'product_clicked'
