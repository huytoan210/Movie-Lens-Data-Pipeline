{{ config(materialized='incremental', unique_key=['user_id','movie_id','tag_ts','tag']) }}

select
  t.user_id,
  t.movie_id,
  t.tag_ts,
  t.tag_date,
  t.tag
from {{ ref('stg_tags') }} t

{% if is_incremental() %}
where t.tag_ts >
  (select coalesce(max(tag_ts), '1970-01-01'::timestamp) from {{ this }})
{% endif %}

