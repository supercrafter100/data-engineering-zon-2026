"""Airflow DAG: laad de metadata van de project-datasets in Postgres.

Mirrors the content of DATA_DOCUMENTATION.md as structured rows in three
catalog tables (metadata_dataset, metadata_column, metadata_relation).
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text


DB_URL = (
    "postgresql+psycopg2://"
    f"{os.getenv('POSTGRES_USER', 'admin')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'password')}@"
    f"{os.getenv('POSTGRES_HOST', 'db')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB', 'combined_weather')}"
)


DATASETS = [
    {
        "name": "consumptie",
        "file_name": "consumptie.csv",
        "description": "Uurlijks elektriciteitsverbruik in Vlaanderen / Belgie.",
        "source": "Elia + VEKA + Kaggle (community)",
        "period_start": "2021-01-01 00:00:00+01",
        "period_end": "2026-02-10 23:00:00+01",
        "row_count": 17759,
        "granularity": "1 hour",
        "timezone": "Europe/Brussels",
        "encoding": "UTF-8",
        "license": "CC-BY (Elia, KMI), Kaggle community",
        "language": "nl",
        "columns": [
            {"name": "tijd", "data_type": "timestamp", "unit": None,
             "description": "Uurstempel (lokale tijd Europe/Brussels).",
             "source_system": None},
            {"name": "Energie vlaanderen zon", "data_type": "double precision",
             "unit": "MWh", "description": "Door zon opgewekt vermogen Vlaanderen.",
             "source_system": "VEKA"},
            {"name": "Energie vlaanderen wind", "data_type": "double precision",
             "unit": "MWh", "description": "Door wind opgewekt vermogen Vlaanderen.",
             "source_system": "VEKA"},
            {"name": "Elia totaal", "data_type": "double precision",
             "unit": "MWh", "description": "Totaal elektriciteitsverbruik op het Elia-net.",
             "source_system": "Elia"},
            {"name": "kaggle prive", "data_type": "double precision",
             "unit": "MWh", "description": "Verbruik prive-aansluitingen.",
             "source_system": "Kaggle"},
            {"name": "kaggle openbaar", "data_type": "double precision",
             "unit": "MWh", "description": "Verbruik openbare aansluitingen.",
             "source_system": "Kaggle"},
        ],
    },
    {
        "name": "productie_comnbined",
        "file_name": "productie_comnbined.csv",
        "description": "Geinjecteerde productie zon en wind, gecombineerd uit Vlaanderen- en Elia-feeds.",
        "source": "Elia Open Data + VEKA",
        "period_start": "2025-02-28 23:00:00+00",
        "period_end": "2026-03-25 22:00:00+00",
        "row_count": 9361,
        "granularity": "1 hour",
        "timezone": "UTC",
        "encoding": "UTF-8",
        "license": "CC-BY (Elia), open (VEKA)",
        "language": "nl",
        "columns": [
            {"name": "tijd", "data_type": "timestamptz", "unit": None,
             "description": "Uurstempel UTC.", "source_system": None},
            {"name": "vlaanderen_zon_kwh", "data_type": "integer", "unit": "kWh",
             "description": "PV-productie Vlaanderen.", "source_system": "VEKA"},
            {"name": "vlaanderen_wind_kwh", "data_type": "integer", "unit": "kWh",
             "description": "Windproductie Vlaanderen.", "source_system": "VEKA"},
            {"name": "elia_zon_kwh", "data_type": "integer", "unit": "kWh",
             "description": "PV-productie via Elia-rapportering (Belgie).",
             "source_system": "Elia"},
            {"name": "elia_wind_kwh", "data_type": "integer", "unit": "kWh",
             "description": "Wind-productie via Elia-rapportering (Belgie).",
             "source_system": "Elia"},
        ],
    },
    {
        "name": "v_wind_alles_compleet",
        "file_name": "v_wind_alles_compleet.csv",
        "description": "Windsnelheidsobservaties uit vier bronnen, gestapeld per uur.",
        "source": "ECMWF (via Open-Meteo) + KMI Ukkel + Antwerpen archief",
        "period_start": "2005-11-10 01:00:00+00",
        "period_end": "2026-03-24 23:00:00+00",
        "row_count": 1137676,
        "granularity": "1 hour",
        "timezone": "UTC",
        "encoding": "UTF-8",
        "license": "Open-Meteo CC-BY-4.0, KMI open data",
        "language": "nl",
        "columns": [
            {"name": "tijdstip", "data_type": "timestamptz", "unit": None,
             "description": "Uurstempel UTC.", "source_system": None},
            {"name": "wind_ecmwf_2026", "data_type": "double precision", "unit": "m/s",
             "description": "Windsnelheid uit ECMWF reanalyse via Open-Meteo.",
             "source_system": "ECMWF"},
            {"name": "wind_kmi_2002", "data_type": "double precision", "unit": "m/s",
             "description": "Windsnelheid KMI archief sinds 2002.",
             "source_system": "KMI"},
            {"name": "wind_ukkel_2024", "data_type": "double precision", "unit": "m/s",
             "description": "Windsnelheid station Ukkel (recent).",
             "source_system": "KMI"},
            {"name": "wind_antwerpen_archive", "data_type": "double precision", "unit": "m/s",
             "description": "Windsnelheid archief station Antwerpen (Deurne).",
             "source_system": "KMI"},
        ],
    },
    {
        "name": "combined_weather",
        "file_name": None,
        "description": "Daggemiddelde kortgolvige straling uit drie bronnen, geproduceerd door de combined_weather_pipeline DAG.",
        "source": "Open-Meteo + KMI AWS + Kaggle",
        "period_start": "2020-01-01 00:00:00+00",
        "period_end": None,
        "row_count": None,
        "granularity": "1 day",
        "timezone": "Europe/Brussels",
        "encoding": "UTF-8",
        "license": "CC-BY-4.0 (Open-Meteo), open (KMI), Kaggle",
        "language": "nl",
        "columns": [
            {"name": "datum", "data_type": "date", "unit": None,
             "description": "Datum (uniek).", "source_system": None},
            {"name": "open_meteo_radiation", "data_type": "double precision",
             "unit": "W/m^2", "description": "Daggemiddelde shortwave radiation Open-Meteo.",
             "source_system": "Open-Meteo"},
            {"name": "kmi_radiation_avg", "data_type": "double precision",
             "unit": "W/m^2", "description": "Daggemiddelde shortwave radiation KMI.",
             "source_system": "KMI"},
            {"name": "kaggle_radiation_avg", "data_type": "double precision",
             "unit": "W/m^2", "description": "Daggemiddelde shortwave radiation Kaggle.",
             "source_system": "Kaggle"},
        ],
    },
]


RELATIONS = [
    {
        "source_dataset": "v_wind_alles_compleet",
        "source_column": "wind_*",
        "target_dataset": "productie_comnbined",
        "target_column": "vlaanderen_wind_kwh",
        "relation_type": "drives",
        "description": "Windsnelheid drijft windproductie aan, kubische relatie P proportioneel aan v^3 tot cut-off.",
    },
    {
        "source_dataset": "combined_weather",
        "source_column": "open_meteo_radiation",
        "target_dataset": "productie_comnbined",
        "target_column": "vlaanderen_zon_kwh",
        "relation_type": "drives",
        "description": "Kortgolvige straling drijft PV-productie aan, quasi-lineair tijdens daglicht.",
    },
    {
        "source_dataset": "productie_comnbined",
        "source_column": "vlaanderen_wind_kwh",
        "target_dataset": "consumptie",
        "target_column": "Elia totaal",
        "relation_type": "supply_vs_demand",
        "description": "Productie en verbruik samen bepalen netto invoer/uitvoer en signalen voor curtailment.",
    },
    {
        "source_dataset": "v_wind_alles_compleet",
        "source_column": "wind_kmi_2002",
        "target_dataset": "v_wind_alles_compleet",
        "target_column": "wind_antwerpen_archive",
        "relation_type": "cross_source_validation",
        "description": "Verschillende meetstations laten data-quality kruisvalidatie toe (Ukkel vs Antwerpen).",
    },
    {
        "source_dataset": "consumptie",
        "source_column": "tijd",
        "target_dataset": "productie_comnbined",
        "target_column": "tijd",
        "relation_type": "join_key",
        "description": "Join op uurstempel, na tijdzone-normalisatie naar UTC.",
    },
]


DDL = """
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
"""


def task_create_schema():
    engine = create_engine(DB_URL, pool_pre_ping=True)
    with engine.begin() as conn:
        for stmt in DDL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))
    print("Metadata-tabellen aangemaakt of al aanwezig.")


def task_upsert_datasets():
    engine = create_engine(DB_URL, pool_pre_ping=True)
    sql = text(
        """
        INSERT INTO metadata_dataset (
            name, file_name, description, source,
            period_start, period_end, row_count,
            granularity, timezone, encoding, license, language, updated_at
        ) VALUES (
            :name, :file_name, :description, :source,
            :period_start, :period_end, :row_count,
            :granularity, :timezone, :encoding, :license, :language, NOW()
        )
        ON CONFLICT (name) DO UPDATE SET
            file_name = EXCLUDED.file_name,
            description = EXCLUDED.description,
            source = EXCLUDED.source,
            period_start = EXCLUDED.period_start,
            period_end = EXCLUDED.period_end,
            row_count = EXCLUDED.row_count,
            granularity = EXCLUDED.granularity,
            timezone = EXCLUDED.timezone,
            encoding = EXCLUDED.encoding,
            license = EXCLUDED.license,
            language = EXCLUDED.language,
            updated_at = NOW();
        """
    )
    with engine.begin() as conn:
        for ds in DATASETS:
            conn.execute(sql, {k: ds.get(k) for k in (
                "name", "file_name", "description", "source",
                "period_start", "period_end", "row_count",
                "granularity", "timezone", "encoding", "license", "language",
            )})
    print(f"{len(DATASETS)} dataset-rijen ge-upsert.")


def task_upsert_columns():
    engine = create_engine(DB_URL, pool_pre_ping=True)
    sql = text(
        """
        INSERT INTO metadata_column (
            dataset_name, name, data_type, unit, description, source_system
        ) VALUES (
            :dataset_name, :name, :data_type, :unit, :description, :source_system
        )
        ON CONFLICT (dataset_name, name) DO UPDATE SET
            data_type = EXCLUDED.data_type,
            unit = EXCLUDED.unit,
            description = EXCLUDED.description,
            source_system = EXCLUDED.source_system;
        """
    )
    total = 0
    with engine.begin() as conn:
        for ds in DATASETS:
            for col in ds["columns"]:
                conn.execute(sql, {
                    "dataset_name": ds["name"],
                    "name": col["name"],
                    "data_type": col.get("data_type"),
                    "unit": col.get("unit"),
                    "description": col.get("description"),
                    "source_system": col.get("source_system"),
                })
                total += 1
    print(f"{total} kolom-rijen ge-upsert.")


def task_upsert_relations():
    engine = create_engine(DB_URL, pool_pre_ping=True)
    sql = text(
        """
        INSERT INTO metadata_relation (
            source_dataset, source_column, target_dataset, target_column,
            relation_type, description
        ) VALUES (
            :source_dataset, :source_column, :target_dataset, :target_column,
            :relation_type, :description
        )
        ON CONFLICT (source_dataset, source_column, target_dataset, target_column)
        DO UPDATE SET
            relation_type = EXCLUDED.relation_type,
            description = EXCLUDED.description;
        """
    )
    with engine.begin() as conn:
        for rel in RELATIONS:
            conn.execute(sql, rel)
    print(f"{len(RELATIONS)} relatie-rijen ge-upsert.")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="metadata_pipeline",
    default_args=default_args,
    description="Laad de metadata van de datasets in de Postgres data catalog.",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["metadata", "catalog"],
) as dag:

    create_schema = PythonOperator(
        task_id="create_schema",
        python_callable=task_create_schema,
    )
    upsert_datasets = PythonOperator(
        task_id="upsert_datasets",
        python_callable=task_upsert_datasets,
    )
    upsert_columns = PythonOperator(
        task_id="upsert_columns",
        python_callable=task_upsert_columns,
    )
    upsert_relations = PythonOperator(
        task_id="upsert_relations",
        python_callable=task_upsert_relations,
    )

    create_schema >> upsert_datasets >> [upsert_columns, upsert_relations]
