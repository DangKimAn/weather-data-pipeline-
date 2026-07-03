CREATE Schema if NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.raw_weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    country VARCHAR(50),
    latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6),
    temperature DECIMAL(5, 2), -- (C or F)
    pressure DECIMAL(10, 2), -- 
    sea_level INTEGER,
    grnd_level INTEGER,
    humidity INTEGER,          -- (%)
    wind_speed DECIMAL(5, 2),  -- 
    wind_deg DECIMAL(5, 2),
    wind_gust DECIMAL(5, 2),
    clouds INTEGER,
    weather_condition VARCHAR(100), -- (example: Rain, Clear)
    descriptions VARCHAR(200),
    sunrise TIMESTAMP,
    sunset TIMESTAMP,
    timezone INTEGER,
    data_timestamp TIMESTAMP,  -- Time that the weather data was recorded
    collected_at TIMESTAMP DEFAULT NOW() --Time that the data was collected from the API
);


