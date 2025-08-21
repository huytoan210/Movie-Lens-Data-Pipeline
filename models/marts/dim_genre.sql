{{ config(materialized='table') }}

with g as (
  select distinct lower(trim(genre)) as genre_name
  from {{ ref('stg_movies') }}
  where genre is not null and genre <> '(no genres listed)'
)
select
  md5(genre_name) as genre_id,
  genre_name
from g
