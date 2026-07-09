

-- DROP TABLE  gold.fact_weather_hourly;
-- DROP TABLE  gold.fact_day;
-- DROP TABLE  gold.dim_location;
-- DROP TABLE  gold.dim_weather_condition;





CREATE or REPLACE PROCEDURE gold.INSERT_dim_location(IN v_last_collected_at TIMESTAMP, OUT p_inserted INT, OUT p_updated INT) LANGUAGE plpgsql AS $$
BEGIN
    WITH upsert AS (
        INSERT INTO gold.dim_location(city, latitude, longitude)
        SELECT city, latitude, longitude FROM (
            SELECT DISTINCT ON (city)
                city,
                latitude,
                longitude,
                collected_at
            FROM silver.transform_weather_data
            WHERE collected_at > v_last_collected_at
            ORDER BY city, collected_at DESC
        ) AS latest_data
        ON CONFLICT (city)
        DO UPDATE SET
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude
        WHERE gold.dim_location.latitude IS DISTINCT FROM EXCLUDED.latitude
           OR gold.dim_location.longitude IS DISTINCT FROM EXCLUDED.longitude

        RETURNING xmax
    )

    SELECT
        COALESCE(COUNT(*) FILTER (WHERE xmax = 0), 0),
        COALESCE(COUNT(*) FILTER (WHERE xmax != 0), 0)
    INTO p_inserted, p_updated
    FROM upsert;
end;
$$;






CREATE or REPLACE PROCEDURE gold.INSERT_dim_weather_condition(IN v_last_collected_at TIMESTAMP, OUT p_inserted INT) LANGUAGE plpgsql as $$
BEGIN

with w as (
    SELECT DISTINCT(weather_condition) from silver.transform_weather_data
    WHERE collected_at >  v_last_collected_at and weather_condition  not in (
        SELECT weather_condition  from gold.dim_weather_condition
    )
)
    INSERT into gold.dim_weather_condition(weather_condition)
    SELECT * from w ;

    get diagnostics p_inserted = row_count ;
end;
$$;








-- CREATE or REPLACE PROCEDURE gold.INSERT_fact_day(IN v_last_collected_at TIMESTAMP, OUT p_inserted INT, OUT p_updated INT) LANGUAGE plpgsql AS $$
-- BEGIN
--     WITH upsert AS (
       

--         INSERT into gold.fact_day(
--     location_id, day, sunrise, sunset, collected_at , source_table
-- )
-- (
-- SELECT g_dl.location_id,  date(data_timestamp),sunrise , sunset , collected_at ,'silver.transform_weather_data'
-- FROM(
-- SELECT city,  data_timestamp  ,
--     CASE 
--     when data_timestamp >= sunrise then sunrise
--     else  null end
--     sunrise , 
--     CASE 
--     when data_timestamp >= sunset then sunset
--     else  null end
--     sunset , collected_at , 'bronze.transform_transform_weather_data', row_number()
--     OVER(
--         PARTITION by (city, date(data_timestamp))
--         ORDER by data_timestamp desc
--     ) as row_number
--     from silver.transform_weather_data
--     WHERE collected_at > v_last_collected_at

-- ) as s_twd LEFT JOIN gold.dim_location as g_dl
-- ON  s_twd.city = g_dl.city
-- WHERE row_number =1)

-- on CONFLICT (location_id , day  )
-- do UPDATE SET 
--     sunrise = COALESCE(EXCLUDED.sunrise, gold.fact_day.sunrise),
--     sunset = COALESCE(EXCLUDED.sunset, gold.fact_day.sunset)
--     WHERE (EXCLUDED.sunrise IS NOT NULL AND EXCLUDED.sunrise != gold.fact_day.sunrise)
--            OR (EXCLUDED.sunset IS NOT NULL AND EXCLUDED.sunset != gold.fact_day.sunset)


--         RETURNING xmax
--     )

--     SELECT
--         COALESCE(COUNT(*) FILTER (WHERE xmax = 0), 0),
--         COALESCE(COUNT(*) FILTER (WHERE xmax != 0), 0)
--     INTO p_inserted, p_updated
--     FROM upsert;
-- end;
-- $$;









CREATE or REPLACE PROCEDURE gold.INSERT_fact_day(IN v_last_collected_at TIMESTAMP, OUT p_inserted INT, OUT p_updated INT) LANGUAGE plpgsql AS $$
BEGIN
    WITH upsert AS (
        INSERT into gold.fact_day(
            location_id, day, sunrise, sunset, collected_at , source_table
        )
        (
            SELECT g_dl.location_id, date(data_timestamp), sunrise, sunset, collected_at, 'silver.transform_weather_data'
            FROM (
                SELECT city, data_timestamp,
                    CASE WHEN data_timestamp >= sunrise THEN sunrise ELSE null END AS sunrise, 
                    CASE WHEN data_timestamp >= sunset THEN sunset ELSE null END AS sunset, 
                    collected_at, 
                    'bronze.transform_transform_weather_data', 
                    row_number() OVER(PARTITION by (city, date(data_timestamp)) ORDER by data_timestamp desc) as row_number
                FROM silver.transform_weather_data
                WHERE collected_at > v_last_collected_at
            ) as s_twd 
            LEFT JOIN gold.dim_location as g_dl ON s_twd.city = g_dl.city
            WHERE row_number = 1
        )
        ON CONFLICT (location_id, day)
        DO UPDATE SET 
            sunrise = COALESCE(EXCLUDED.sunrise, gold.fact_day.sunrise),
            sunset = COALESCE(EXCLUDED.sunset, gold.fact_day.sunset)
        WHERE (EXCLUDED.sunrise IS NOT NULL AND EXCLUDED.sunrise != gold.fact_day.sunrise)
           OR (EXCLUDED.sunset IS NOT NULL AND EXCLUDED.sunset != gold.fact_day.sunset)

        -- SỬA Ở ĐÂY: Thay xmax bằng một cột bình thường
        RETURNING location_id 
    )

    -- SỬA Ở ĐÂY: Gán tổng số dòng xử lý vào p_inserted, và gán 0 cho p_updated
    SELECT
        COUNT(*), 
        0         
    INTO p_inserted, p_updated
    FROM upsert;
END;
$$;









CREATE or REPLACE PROCEDURE gold.INSERT_fact_weather_hourly(IN v_last_collected_at TIMESTAMP, OUT p_inserted INT) LANGUAGE plpgsql as $$
BEGIN

insert into gold.fact_weather_hourly(
location_id ,
temperature,
pressure,
sea_level,
grnd_level,
humidity,
wind_speed,
wind_deg,
wind_gust,
clouds,
weather_condition_id ,
data_timestamp,
collected_at,
source_table
)
SELECT
dl.location_id location_id ,
    temperature,
pressure,
sea_level,
grnd_level,
humidity,
wind_speed,
wind_deg,
wind_gust,
clouds,
dwc.weather_condition_id weather_condition_id ,
data_timestamp,
collected_at,
'silver.transform_weather_data' source_table
 from (silver.transform_weather_data as twd 
 INNER JOIN gold.dim_location as dl 
 on twd.city = dl.city ) INNER JOIN gold.dim_weather_condition dwc 
 on twd.weather_condition = dwc.weather_condition 
WHERE collected_at > v_last_collected_at;

    get diagnostics p_inserted = row_count ;
end;
$$;







-- DROp PROCEDURE gold.insert_data;


create or REPLACE PROCEDURE gold.insert_data(
OUT total_dim_located_inserted INT, 
OUT total_dim_located_upddated INT,
OUT total_dim_weather_condition_inserted INT,
OUT total_fact_day_updated INT, 
OUT total_fact_day_inserted INT,
OUT total_fact_weather_hourly_inserted INT
) 
LANGUAGE plpgsql as $$
DECLARE
    v_last_collected_at TIMESTAMP;
begin

    SELECT COALESCE(
        (SELECT MAX(collected_at) FROM gold.fact_weather_hourly),
        '2000-01-01 00:00:00'::timestamp
    ) INTO v_last_collected_at;

    call gold.INSERT_dim_location(v_last_collected_at,
    total_dim_located_inserted,
    total_dim_located_upddated);
    call gold.INSERT_dim_weather_condition(v_last_collected_at,total_dim_weather_condition_inserted);

    call gold.INSERT_fact_day(v_last_collected_at , total_fact_day_inserted, total_fact_day_updated);
    call gold.INSERT_fact_weather_hourly(v_last_collected_at , total_fact_weather_hourly_inserted);
end;
$$;

-- call gold.insert_data(NULL , NULL , NULL, NULL , NULL, NULL);

