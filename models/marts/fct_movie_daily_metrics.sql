{{ config(materialized='table') }}

with r as (
  select movie_id, rating_date, count(*) as rating_cnt, avg(rating_value) as rating_avg
  from {{ ref('fct_ratings') }}
  group by 1,2
),
t as (
  select movie_id, tag_date, count(*) as tag_cnt
  from {{ ref('fct_tags') }}
  group by 1,2
)
select
  coalesce(r.movie_id, t.movie_id) as movie_id,
  coalesce(r.rating_date, t.tag_date) as date,
  r.rating_cnt,
  r.rating_avg,
  t.tag_cnt
from r
full outer join t
  on r.movie_id = t.movie_id and r.rating_date = t.tag_date
