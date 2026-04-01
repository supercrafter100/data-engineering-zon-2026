"""
One Airflow DAG per CSV file in /data/datasets.

Each DAG has a single task that reads the CSV and writes it to Postgres,
mirroring import_csvs.py so datetime handling is identical (no XCom
serialisation that would corrupt timezone-aware timestamps).
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine

DATASETS_DIR = Path("/data/datasets")

DB_URL = (
    "postgresql+psycopg2://"
    f"{os.getenv('POSTGRES_USER', 'admin')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'password')}@"
    f"{os.getenv('POSTGRES_HOST', 'db')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB', 'combined_weather')}"
)

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _find_date_column(columns: list[str]) -> str | None:
    lookup = {c.strip().lower(): c for c in columns}
    for candidate in ("tijd", "tijdstip"):
        if candidate in lookup:
            return lookup[candidate]
    return None


def make_import_task(csv_path: Path):
    table_name = csv_path.stem

    def import_csv():
        df = pd.read_csv(csv_path)

        date_col = _find_date_column(df.columns.tolist())
        if date_col:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            df = df.dropna(subset=[date_col])

        engine = create_engine(DB_URL, pool_pre_ping=True)
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"{csv_path.name} -> tabel '{table_name}' ({len(df)} rijen)")

    return import_csv


# Build one DAG per CSV file found at import time.
# Airflow discovers DAGs by scanning the module's global namespace.
for csv_file in sorted(DATASETS_DIR.glob("*.csv")):
    table = csv_file.stem
    dag_id = f"csv_pipeline_{table}"

    dag = DAG(
        dag_id=dag_id,
        default_args=DEFAULT_ARGS,
        description=f"Import {csv_file.name} into table '{table}'",
        start_date=datetime(2025, 1, 1),
        schedule_interval="@daily",
        catchup=False,
        tags=["csv", "import"],
    )

    with dag:
        PythonOperator(
            task_id=f"import_{table}",
            python_callable=make_import_task(csv_file),
        )

    # Register in module globals so Airflow can discover the DAG
    globals()[dag_id] = dag
