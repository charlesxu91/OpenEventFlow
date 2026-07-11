CREATE DATABASE IF NOT EXISTS openeventflow;

CREATE TABLE IF NOT EXISTS openeventflow.ods_snowplow_enriched_events
(
    event_id String,
    event_name String,
    schema String,
    collector_time Nullable(UInt64),
    enriched_at Nullable(UInt64),
    event_json String
)
ENGINE = MergeTree
ORDER BY (event_name, event_id);

CREATE TABLE IF NOT EXISTS openeventflow.ods_snowplow_bad_events
(
    event_id Nullable(String),
    reason String,
    property Nullable(String),
    bad_event_json String,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (reason, created_at);

CREATE TABLE IF NOT EXISTS openeventflow.dwd_app_behavior_events
(
    event_id String,
    event_name String,
    schema String,
    user_id Nullable(String),
    anonymous_id Nullable(String),
    app_id String,
    platform String,
    event_date Date,
    event_time UInt64,
    collector_time Nullable(UInt64),
    enriched_at Nullable(UInt64),
    properties String
)
ENGINE = MergeTree
ORDER BY (event_date, event_name, event_time, event_id);

CREATE TABLE IF NOT EXISTS openeventflow.fact_product_exposures
(
    event_id String,
    event_date Date,
    user_id Nullable(String),
    anonymous_id Nullable(String),
    product_id String,
    page String,
    position Int64,
    exposure_id String,
    visible_ratio Float64,
    duration_ms Int64,
    recommend_trace_id Nullable(String),
    event_time UInt64
)
ENGINE = MergeTree
ORDER BY (event_date, page, product_id, event_time);

CREATE TABLE IF NOT EXISTS openeventflow.fact_product_clicks
(
    event_id String,
    event_date Date,
    user_id Nullable(String),
    anonymous_id Nullable(String),
    product_id String,
    page String,
    position Int64,
    click_id String,
    recommend_trace_id Nullable(String),
    event_time UInt64
)
ENGINE = MergeTree
ORDER BY (event_date, page, product_id, event_time);

CREATE TABLE IF NOT EXISTS openeventflow.fact_page_stays
(
    event_id String,
    event_date Date,
    user_id Nullable(String),
    anonymous_id Nullable(String),
    page String,
    duration_ms Int64,
    stay_id String,
    exit_reason Nullable(String),
    event_time UInt64
)
ENGINE = MergeTree
ORDER BY (event_date, page, event_time);

CREATE TABLE IF NOT EXISTS openeventflow.fact_cart_adds
(
    event_id String,
    event_date Date,
    user_id Nullable(String),
    anonymous_id Nullable(String),
    product_id String,
    sku_id String,
    quantity Int64,
    price Float64,
    currency Nullable(String),
    gmv Float64,
    event_time UInt64
)
ENGINE = MergeTree
ORDER BY (event_date, product_id, event_time);

CREATE TABLE IF NOT EXISTS openeventflow.fact_recommendation_events
(
    event_id String,
    event_name String,
    event_date Date,
    event_time UInt64,
    user_id Nullable(String),
    anonymous_id Nullable(String),
    request_id Nullable(String),
    impression_id Nullable(String),
    delivery_id Nullable(String),
    product_id String,
    sku_id Nullable(String),
    surface Nullable(String),
    position Nullable(Int64),
    candidate_source Nullable(String),
    model_version Nullable(String),
    feature_set_version Nullable(String),
    experiment_id Nullable(String),
    experiment_treatment Nullable(String),
    recommendation_generation Nullable(String)
)
ENGINE = MergeTree
ORDER BY (event_date, event_name, product_id, event_time);

CREATE TABLE IF NOT EXISTS openeventflow.fact_order_events
(
    event_id String,
    event_name String,
    event_date Date,
    event_time UInt64,
    user_id Nullable(String),
    order_id String,
    order_line_id String,
    product_id String,
    sku_id String,
    quantity Int64,
    amount Float64,
    currency Nullable(String),
    request_id Nullable(String),
    impression_id Nullable(String),
    delivery_id Nullable(String)
)
ENGINE = MergeTree
ORDER BY (event_date, order_id, order_line_id, event_time);

CREATE TABLE IF NOT EXISTS openeventflow.ads_product_behavior_daily
(
    event_date Date,
    product_id String,
    exposures UInt64,
    clicks UInt64,
    cart_adds UInt64,
    cart_quantity UInt64,
    cart_gmv Float64
)
ENGINE = SummingMergeTree
ORDER BY (event_date, product_id);

CREATE TABLE IF NOT EXISTS openeventflow.fact_video_exposures
(
    event_id String,
    event_date Date,
    user_id Nullable(String),
    anonymous_id Nullable(String),
    video_id String,
    author_id Nullable(String),
    page String,
    position Int64,
    exposure_id String,
    visible_ratio Float64,
    duration_ms Int64,
    recommend_trace_id Nullable(String),
    event_time UInt64
)
ENGINE = MergeTree
ORDER BY (event_date, page, video_id, event_time);

CREATE TABLE IF NOT EXISTS openeventflow.fact_video_plays
(
    event_id String,
    event_date Date,
    user_id Nullable(String),
    anonymous_id Nullable(String),
    video_id String,
    author_id Nullable(String),
    page String,
    position Int64,
    play_id String,
    autoplay Nullable(Bool),
    network_type Nullable(String),
    recommend_trace_id Nullable(String),
    event_time UInt64
)
ENGINE = MergeTree
ORDER BY (event_date, page, video_id, event_time);

CREATE TABLE IF NOT EXISTS openeventflow.fact_video_watches
(
    event_id String,
    event_date Date,
    user_id Nullable(String),
    anonymous_id Nullable(String),
    video_id String,
    author_id Nullable(String),
    play_id String,
    watch_id String,
    duration_ms Int64,
    play_duration_ms Nullable(Int64),
    completion_rate Nullable(Float64),
    completed Bool,
    exit_reason Nullable(String),
    event_time UInt64
)
ENGINE = MergeTree
ORDER BY (event_date, video_id, event_time);

CREATE TABLE IF NOT EXISTS openeventflow.fact_video_engagements
(
    event_id String,
    event_date Date,
    user_id Nullable(String),
    anonymous_id Nullable(String),
    video_id String,
    author_id Nullable(String),
    play_id Nullable(String),
    engagement_id String,
    action String,
    event_time UInt64
)
ENGINE = MergeTree
ORDER BY (event_date, video_id, action, event_time);

CREATE TABLE IF NOT EXISTS openeventflow.ads_video_behavior_daily
(
    event_date Date,
    video_id String,
    author_id Nullable(String),
    exposures UInt64,
    plays UInt64,
    watch_ms UInt64,
    completed_plays UInt64,
    likes UInt64,
    comments UInt64,
    shares UInt64,
    follows UInt64
)
ENGINE = SummingMergeTree
ORDER BY (event_date, video_id);
