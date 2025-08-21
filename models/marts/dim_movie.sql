{{ config(materialized='table') }}

with m as (
  select distinct
    s.movie_id,
    s.title_clean as title,
    s.release_year
  from {{ ref('stg_movies') }} s
),
links as (
  select movie_id, imdb_id, tmdb_id
  from {{ ref('stg_links') }}
)
select
  m.movie_id,
  m.title,
  m.release_year,
  l.imdb_id,
  l.tmdb_id
from m
left join links l using(movie_id)
