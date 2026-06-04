WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2016-01-01' as date)",
        end_date="cast('2019-12-31' as date)"
    ) }}
)

SELECT
    date_day                                    AS date_id,
    date_day                                    AS full_date,
    EXTRACT(year  FROM date_day)::INT           AS year,
    EXTRACT(quarter FROM date_day)::INT         AS quarter,
    EXTRACT(month FROM date_day)::INT           AS month,
    MONTHNAME(date_day)                         AS month_name,
    EXTRACT(week  FROM date_day)::INT           AS week_of_year,
    EXTRACT(day   FROM date_day)::INT           AS day_of_month,
    DAYNAME(date_day)                           AS day_name,
    DAYOFWEEK(date_day)::INT                    AS day_of_week,
    CASE WHEN DAYOFWEEK(date_day) IN (1, 7)
        THEN TRUE ELSE FALSE END                AS is_weekend
FROM date_spine
