WITH temp_daily AS (
    SELECT * 
    FROM {{ref('staging_temp')}}
),
add_weekday AS (
    SELECT *,
        DATE_PART('day', date) AS weekday,
        DATE_PART('DD', date). AS day_num
    FROM temp_daily
)
SELECT *
FROM add_weekday