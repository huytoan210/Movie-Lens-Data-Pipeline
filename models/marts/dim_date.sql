WITH date_series AS (
  SELECT
    DATEADD(DAY, SEQ4(), DATE '1995-01-01')::DATE AS date_id
  FROM TABLE(GENERATOR(ROWCOUNT => 10000)) AS g
  WHERE DATEADD(DAY, SEQ4(), DATE '1995-01-01') <= CURRENT_DATE
)
SELECT 
    date_id,
    DAY(date_id) AS day,
    MONTH(date_id) AS month,
    QUARTER(date_id) AS quarter,
    YEAR(date_id) AS year,
    DAYOFWEEK(date_id) AS day_of_week,
    CASE WHEN DAYOFWEEK(date_id) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend,
    NULL AS is_holiday,
    WEEK(date_id) AS week_of_year
FROM date_series
