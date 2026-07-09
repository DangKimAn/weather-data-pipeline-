

create  Schema if not exists gold;

CREATE TABLE IF NOT EXISTS gold.dim_weather_condition (
    weather_condition_id SERIAL PRIMARY KEY,
    weather_condition VARCHAR(100) unique,
    created_at TIMESTAMP DEFAULT now()
);



create table if not exists gold.dim_location(
    location_id SERIAL PRIMARY key,
    city VARCHAR(100) UNIQUE,
    latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6),
    created_at TIMESTAMP DEFAULT now()
);


create table if not exists gold.fact_weather_hourly(
    id SERIAL ,
    location_id int  references gold.dim_location(location_id),
    temperature DECIMAL(5, 2),
    pressure DECIMAL(10, 2),
    sea_level BIGINT,
    grnd_level BIGINT,
    humidity BIGINT,
    wind_speed DECIMAL(5, 2),
    wind_deg DECIMAL(5, 2),
    wind_gust DECIMAL(5, 2),
    clouds BIGINT,
    weather_condition_id  int  references gold.dim_weather_condition(weather_condition_id),
    data_timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT now(),
    collected_at TIMESTAMP,
    source_table VARCHAR(100),
     PRIMARY KEY (id, data_timestamp)
)PARTITION BY RANGE (data_timestamp);



create table if not exists gold.fact_day(
    fact_day_id SERIAL  ,
    location_id int  references gold.dim_location(location_id) NOT NULL,
    day date not null ,
    sunrise TIMESTAMP,
    sunset TIMESTAMP,
    created_at TIMESTAMP DEFAULT now(),
    collected_at TIMESTAMP,
    source_table VARCHAR(100),
    PRIMARY KEY (fact_day_id, day),
    CONSTRAINT unique_location_day UNIQUE (location_id, day)
)PARTITION BY RANGE (day);



CREATE TABLE if not exists gold.fact_weather_hourly_2026_07 PARTITION OF gold.fact_weather_hourly
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');


CREATE TABLE if not exists gold.fact_weather_hourly_2026_08 PARTITION OF gold.fact_weather_hourly
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');


CREATE TABLE if not exists gold.fact_weather_hourly_2026_09 PARTITION OF gold.fact_weather_hourly
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');


CREATE TABLE if not exists gold.fact_weather_hourly_2026_10 PARTITION OF gold.fact_weather_hourly
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');


CREATE TABLE if not exists gold.fact_weather_hourly_2026_11 PARTITION OF gold.fact_weather_hourly
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');


CREATE TABLE if not exists gold.fact_weather_hourly_2026_12 PARTITION OF gold.fact_weather_hourly
    FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');


-- Tạo sẵn partition cho năm 2026
CREATE TABLE if not exists gold.fact_day_2026 PARTITION OF gold.fact_day
    FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

    -- Tạo sẵn partition cho năm 2026
CREATE TABLE if not exists gold.fact_day_2027 PARTITION OF gold.fact_day
    FOR VALUES FROM ('2027-01-01') TO ('2028-01-01');