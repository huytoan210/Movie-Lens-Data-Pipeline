{{ config(materialized='table') }}

with u as (
  select distinct user_id from {{ ref('stg_ratings') }}
),
stats as (
  select
    user_id,
    min(rating_date) as first_activity_date,
    max(rating_date) as last_activity_date,
    count(*) as rating_count
  from {{ ref('stg_ratings') }}
  group by 1
)
select
  u.user_id,
  s.first_activity_date,
  s.last_activity_date,
  s.rating_count
from u
left join stats s using(user_id)
