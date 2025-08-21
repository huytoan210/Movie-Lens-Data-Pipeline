{{ config(materialized='view') }}

with base as (
  select
    movieId as movie_id,
    title,
    {{ extract_year_from_title('title') }} as release_year,
    {{ strip_year_from_title('title') }} as title_clean,
    genres
  from {{ source('raw','RAW_MOVIES') }}
),
explode_genres as (
  select
    movie_id,
    title,
    release_year,
    title_clean,
    value::string as genre
  from base,
  lateral flatten(input => split(genres, '|')) as g
)
select * from explode_genres