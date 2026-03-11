CREATE TABLE IF NOT EXISTS combined_weather (
    id SERIAL PRIMARY KEY,
    datum DATE NOT NULL UNIQUE,
    open_meteo_radiation DOUBLE PRECISION,
    kmi_radiation_avg DOUBLE PRECISION,
    kaggle_radiation_avg DOUBLE PRECISION
);