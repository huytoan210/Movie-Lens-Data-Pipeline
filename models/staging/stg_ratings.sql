{{ config(materialized='incremental', unique_key=['user_id','movie_id','rating_ts']) }}

select
  userId as user_id,
  movieId as movie_id,
  rating as rating_value,
  {{ epoch_to_ts('timestamp') }} as rating_ts,
  to_date({{ epoch_to_ts('timestamp') }}) as rating_date
from {{ source('raw','RAW_RATINGS') }}

{% if is_incremental() %}
-- nếu RAW có append theo thời gian, có thể filter theo max timestamp đã build
where {{ epoch_to_ts('timestamp') }} >
  (select coalesce(max(rating_ts), '1970-01-01'::timestamp) from {{ this }})
{% endif %}