with total_avg as (
    SELECT
    city, country, lat, lon,
    avg(avgtemp_c) as avg_temp_week,
    max(maxtemp_c) as max_temp_week,
    min(mintemp_c) as mix_temp_week
    from {{ref("prep_temp")}}
    Group by city, country, week, lat, lon
    )
SELECT * from total_avg