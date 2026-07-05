select
  event_date,
  product_id,
  sum(exposures) as exposures,
  sum(clicks) as clicks,
  sum(cart_adds) as cart_adds,
  sum(cart_quantity) as cart_quantity,
  sum(cart_gmv) as cart_gmv
from (
  select
    event_date,
    product_id,
    count() as exposures,
    0 as clicks,
    0 as cart_adds,
    0 as cart_quantity,
    0.0 as cart_gmv
  from {{ ref('fact_product_exposures') }}
  group by event_date, product_id

  union all

  select
    event_date,
    product_id,
    0 as exposures,
    count() as clicks,
    0 as cart_adds,
    0 as cart_quantity,
    0.0 as cart_gmv
  from {{ ref('fact_product_clicks') }}
  group by event_date, product_id

  union all

  select
    event_date,
    product_id,
    0 as exposures,
    0 as clicks,
    count() as cart_adds,
    toUInt64(sum(quantity)) as cart_quantity,
    toFloat64(sum(gmv)) as cart_gmv
  from {{ ref('fact_cart_adds') }}
  group by event_date, product_id
)
group by event_date, product_id
