select
  event_id,
  event_name,
  schema,
  JSONExtractString(event_json, 'context', 'user', 'user_id') as user_id,
  JSONExtractString(event_json, 'context', 'user', 'anonymous_id') as anonymous_id,
  JSONExtractString(event_json, 'context', 'app', 'app_id') as app_id,
  JSONExtractString(event_json, 'context', 'app', 'platform') as platform,
  toDate(fromUnixTimestamp64Milli(JSONExtractUInt(event_json, 'client_time'))) as event_date,
  JSONExtractUInt(event_json, 'client_time') as event_time,
  collector_time,
  enriched_at,
  JSONExtractRaw(event_json, 'properties') as properties
from {{ source('openeventflow', 'ods_snowplow_enriched_events') }}
