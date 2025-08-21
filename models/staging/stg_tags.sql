{{ config(materialized='incremental', unique_key=['user_id','movie_id','tag_ts','tag']) }}

select
  userId as user_id,
  movieId as movie_id,
  tag,
  {{ epoch_to_ts('timestamp') }} as tag_ts,
  to_date({{ epoch_to_ts('timestamp') }}) as tag_date
from {{ source('raw','RAW_TAGS') }}

{% if is_incremental() %}
where {{ epoch_to_ts('timestamp') }} >
  (select coalesce(max(tag_ts), '1970-01-01'::timestamp) from {{ this }})
{% endif %}