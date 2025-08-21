{{ config(materialized='table') }}

with mg as (
  select distinct
    sm.movie_id,
    lower(trim(sm.genre)) as genre_name
  from {{ ref('stg_movies') }} sm
  where sm.genre is not null and sm.genre <> '(no genres listed)'
),
dg as (
  select * from {{ ref('dim_genre') }}
)
select
  mg.movie_id,
  dg.genre_id
from mg
join dg on dg.genre_name = mg.genre_name
