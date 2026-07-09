
create or REPLACE PROCEDURE silver.transform_data()
LANGUAGE plpgsql 
as 
$$
DECLARE
    v_last_collected_at TIMESTAMP;
begin 
    SELECT COALESCE(
        (SELECT MAX(collected_at) FROM silver.transform_weather_data), 
        '2000-01-01 00:00:00'::timestamp
    ) INTO v_last_collected_at;


with tmp_transform_filter_by_id as(
    SELECT *, 
    row_number()
    OVER(
        PARTITION by id
        order by collected_at desc 
    ) as num_ber   
    from bronze.raw_weather_data
    WHERE collected_at > v_last_collected_at    
      
)


    INSERT into silver.TRANSFORM_weather_data (

        id ,
    city ,
    latitude ,
    longitude ,
    temperature ,
    pressure,  
    sea_level ,
    grnd_level ,
    humidity , 
    wind_speed ,  
    wind_deg ,
    wind_gust ,
    clouds ,
    weather_condition, 
    descriptions ,
    sunrise ,
    sunset ,
    timezone ,
    data_timestamp ,  
    source_table ,
    collected_at 
    )

SELECT 
      id ,
    CONCAT_WS(', ', TRIM(city), upper(TRIM(country))),
    latitude ,
    longitude ,
    temperature ,
    CASE WHEN pressure <= 0 THEN NULL ELSE pressure END AS pressure,
    CASE WHEN sea_level <= 0 THEN NULL ELSE sea_level END AS sea_level,
    CASE WHEN grnd_level <= 0 THEN NULL ELSE grnd_level END AS grnd_level,
    
    CASE WHEN humidity < 0 OR humidity > 100 THEN NULL ELSE humidity END AS humidity,
    CASE WHEN clouds < 0 OR clouds > 100 THEN NULL ELSE clouds END AS clouds,
    
    CASE WHEN wind_speed < 0 THEN NULL ELSE wind_speed END AS wind_speed,
    CASE WHEN wind_gust < 0 THEN NULL ELSE wind_gust END AS wind_gust,
    
    CASE WHEN wind_deg < 0 OR wind_deg > 360 THEN NULL ELSE wind_deg END AS wind_deg,
    COALESCE(weather_condition, 'N/A'),
    COALESCE(descriptions, 'N/A'),
    sunrise ,
    sunset ,
    timezone ,
    data_timestamp ,  
    'bronze.raw_table_weather' ,
    collected_at 
    from tmp_transform_filter_by_id 
    WHERE num_ber =1 and (longitude BETWEEN -180 and 180 )
    and (latitude BETWEEN -90 and 90 )
    and timezone is not null ;
end;
$$;
