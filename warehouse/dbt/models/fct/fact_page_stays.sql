select
  event_id,
  event_date,
  user_id,
  anonymous_id,
  JSONExtractString(properties, 'page') as page,
  JSONExtractInt(properties, 'duration_ms') as duration_ms,
  JSONExtractString(properties, 'stay_id') as stay_id,
  JSONExtractString(properties, 'exit_reason') as exit_reason,
  event_time
from {{ ref('dwd_app_behavior_events') }}
where event_name = 'page_stay'
