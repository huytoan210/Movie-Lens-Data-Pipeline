{{ config(materialized='view') }}

select
  movieId as movie_id,
  imdbId as imdb_id,
  tmdbId as tmdb_id
from {{ source('raw','RAW_LINKS') }}