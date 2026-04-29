CREATE TABLE IF NOT EXISTS combined_weather (
    id SERIAL PRIMARY KEY,
    datum DATE NOT NULL UNIQUE,
    open_meteo_radiation DOUBLE PRECISION,
    kmi_radiation_avg DOUBLE PRECISION,
    kaggle_radiation_avg DOUBLE PRECISION
);

-- Data catalog: metadata over de datasets in dit project.
CREATE TABLE IF NOT EXISTS metadata_dataset (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    file_name TEXT,
    description TEXT,
    source TEXT,
    period_start TIMESTAMPTZ,
    period_end TIMESTAMPTZ,
    row_count BIGINT,
    granularity TEXT,
    timezone TEXT,
    encoding TEXT,
    license TEXT,
    language TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS metadata_column (
    id SERIAL PRIMARY KEY,
    dataset_name TEXT NOT NULL REFERENCES metadata_dataset(name) ON DELETE CASCADE,
    name TEXT NOT NULL,
    data_type TEXT,
    unit TEXT,
    description TEXT,
    source_system TEXT,
    UNIQUE (dataset_name, name)
);

CREATE TABLE IF NOT EXISTS metadata_relation (
    id SERIAL PRIMARY KEY,
    source_dataset TEXT NOT NULL,
    source_column TEXT,
    target_dataset TEXT NOT NULL,
    target_column TEXT,
    relation_type TEXT,
    description TEXT,
    UNIQUE (source_dataset, source_column, target_dataset, target_column)
);