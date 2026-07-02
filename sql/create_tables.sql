CREATE TABLE IF NOT EXISTS raw_weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    country VARCHAR(50),
    latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6),
    temperature DECIMAL(5, 2), -- (C or F)
    humidity INTEGER,          -- (%)
    wind_speed DECIMAL(5, 2),  -- 
    weather_condition VARCHAR(100), -- (example: Rain, Clear)
    data_timestamp TIMESTAMP,  -- Time that the weather data was recorded
    collected_at TIMESTAMP DEFAULT NOW(), --Time that the data was collected from the API
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Time that the data was inserted into db
    raw_data JSON NOT NULL,
    status_pipeline VARCHAR(20) DEFAULT 'SUCCESS'
);

