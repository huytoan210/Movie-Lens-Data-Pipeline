{{ config(materialized='incremental', unique_key=['user_id','movie_id','rating_ts']) }}

select
  r.user_id,
  r.movie_id,
  r.rating_ts,
  r.rating_date,
  r.rating_value
from {{ ref('stg_ratings') }} r

{% if is_incremental() %}
where r.rating_ts >
  (select coalesce(max(rating_ts), '1970-01-01'::timestamp) from {{ this }})
{% endif %}

