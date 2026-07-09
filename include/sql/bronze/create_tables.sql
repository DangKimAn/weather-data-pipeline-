CREATE Schema if NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.raw_weather_data (
    id SERIAL ,
    city VARCHAR(100),
    country VARCHAR(50),
    latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6),
    temperature DECIMAL(5, 2), -- (C or F)
    pressure DECIMAL(10, 2), -- 
    sea_level BIGINT,
    grnd_level BIGINT,
    humidity BIGINT,          -- (%)
    wind_speed DECIMAL(5, 2),  -- 
    wind_deg DECIMAL(5, 2),
    wind_gust DECIMAL(5, 2),
    clouds BIGINT,
    weather_condition VARCHAR(100), -- (example: Rain, Clear)
    descriptions VARCHAR(200),
    sunrise TIMESTAMP,
    sunset TIMESTAMP,
    timezone BIGINT,
    data_timestamp TIMESTAMP NOT NULL,  -- Time that the weather data was recorded
    collected_at TIMESTAMP DEFAULT NOW(), --Time that the data was collected from the API
    PRIMARY KEY (id, data_timestamp)
)PARTITION BY RANGE (data_timestamp);

CREATE TABLE if not exists bronze.raw_weather_data_2026_07 PARTITION OF bronze.raw_weather_data
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');


CREATE TABLE if not exists bronze.raw_weather_data_2026_08 PARTITION OF bronze.raw_weather_data
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');


CREATE TABLE if not exists bronze.raw_weather_data_2026_09 PARTITION OF bronze.raw_weather_data
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');


CREATE TABLE if not exists bronze.raw_weather_data_2026_10 PARTITION OF bronze.raw_weather_data
    FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');


CREATE TABLE if not exists bronze.raw_weather_data_2026_11 PARTITION OF bronze.raw_weather_data
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');


CREATE TABLE if not exists bronze.raw_weather_data_2026_12 PARTITION OF bronze.raw_weather_data
    FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');

