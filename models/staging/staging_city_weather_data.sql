--DROP TABLE IF EXISTS city_weather_data;
--create table city_weather_data as (
WITH city_weather_data AS (
SELECT 	
		-- GENERAL DATA
		(extracted_data -> 'location' -> 'name')::VARCHAR  AS city,
		(extracted_data -> 'location' -> 'region')::VARCHAR  AS region,
		(extracted_data -> 'location' -> 'country')::VARCHAR  AS country,
        ((extracted_data -> 'location' -> 'lat')::VARCHAR)::NUMERIC  AS lat, 
        ((extracted_data -> 'location' -> 'lon')::VARCHAR)::NUMERIC  AS lon,
		-- DAY DATA
		((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'date')::VARCHAR)::date  AS date,
		((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'maxtemp_c')::VARCHAR)::FLOAT AS maxtemp_c,
		((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'mintemp_c')::VARCHAR)::FLOAT AS mintemp_c,
		((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'avgtemp_c')::VARCHAR)::FLOAT AS avgtemp_c,
		((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'day' -> 'uv')::VARCHAR)::FLOAT AS uv,
		-- ASTRO DATA
		((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'astro' -> 'sunrise')::VARCHAR)::FLOAT AS sunrise,
		((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'astro' -> 'sunset')::VARCHAR)::FLOAT AS sunset,
		((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'astro' -> 'moonrise')::VARCHAR)::FLOAT AS moonrise,
		((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'astro' -> 'moonset')::VARCHAR)::FLOAT AS moonset,
		((extracted_data -> 'forecast' -> 'forecastday' -> 0 -> 'astro' -> 'moon_illumination')::VARCHAR)::FLOAT AS moonlight        
FROM {{source("staging", "raw_temp")}})

SELECT * FROM city_weather_data ORDER BY city, date ASC