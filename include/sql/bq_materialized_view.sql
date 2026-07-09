

insert into gold.dim_location(city, latitude, longitude)

SELECT city , latitude , longitude from(
 SELECT  DISTINCT on (city) city , latitude , longitude ,collected_at from silver.transform_weather_data
    ORDER BY city,  collected_at DESC
)
on CONFLICT (city)
DO UPDATE SET  
latitude = EXCLUDED.latitude,
longitude = EXCLUDED.longitude
WHERE gold.dim_location.latitude != EXCLUDED.latitude or 
gold.dim_location.longitude != EXCLUDED.longitude;

SELECT city, latitude , longitude from(
 SELECT  DISTINCT on (city) city , latitude , longitude ,collected_at from silver.transform_weather_data
    ORDER BY city, collected_at DESC
);

call  gold.insert_data(NULL , NULL , NULL, NULL , NULL, NULL);


SELECT *  from 
information_schema.tables
where table_schema='gold';

DROp table gold.fact_day;
DROp table gold.fact_weather_hourly;
DROp table gold.dim_location;
DROp table gold.dim_weather_condition;


