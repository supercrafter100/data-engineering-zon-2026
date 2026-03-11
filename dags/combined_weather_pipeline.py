from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import io
import os
from datetime import date
from sqlalchemy import create_engine, text

# === Database configuratie ===
DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "combined_weather")
DB_USER = os.getenv("POSTGRES_USER", "admin")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "password")
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

START_DATE = "2020-01-01"
LATITUDE = 51.2205
LONGITUDE = 4.4003


def get_engine():
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return engine


def task_fetch_open_meteo(**context):
    """Haal historische uurlijkse straling op van Open-Meteo en bereken daggemiddelde (W/m²)."""
    print("Dataset 1: Open-Meteo historische straling ophalen...")

    end = (date.today() - timedelta(days=5)).isoformat()
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": START_DATE,
        "end_date": end,
        "hourly": "shortwave_radiation",
        "timezone": "Europe/Brussels",
    }

    resp = requests.get(url, params=params, timeout=120)
    resp.raise_for_status()
    data = resp.json()

    df = pd.DataFrame(
        {
            "datum": pd.to_datetime(data["hourly"]["time"]),
            "radiation": data["hourly"]["shortwave_radiation"],
        }
    )
    df["datum"] = df["datum"].dt.normalize()
    daily = df.groupby("datum")["radiation"].mean().reset_index()
    daily.columns = ["datum", "open_meteo_radiation"]

    print(f"   Open-Meteo: {len(daily)} dagen opgehaald.")
    context["ti"].xcom_push(key="open_meteo", value=daily.to_json(date_format="iso"))


def task_fetch_kmi(**context):
    """Haal KMI AWS uurlijkse data op en bereken daggemiddelde straling (W/m²)."""
    print("Dataset 2: KMI AWS straling ophalen...")

    url = "http://opendata.meteo.be/geoserver/aws/wfs"
    params = {
        "service": "WFS",
        "version": "1.1.0",
        "request": "GetFeature",
        "typeName": "aws:aws_1hour",
        "outputFormat": "csv",
    }

    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()

    df = pd.read_csv(io.BytesIO(resp.content), encoding="ISO-8859-1")
    df.columns = [c.split(":")[-1].replace('"', "").strip().lower() for c in df.columns]

    time_col = next(
        (c for c in ["timestamp", "time", "datetime", "date"] if c in df.columns), None
    )
    rad_col = next(
        (
            c
            for c in [
                "short_wave_from_sky_avg",
                "short_wave_from_sky",
                "sw_radiation",
                "shortwave_radiation",
                "global_radiation",
                "radiation",
            ]
            if c in df.columns
        ),
        None,
    )

    if not time_col or not rad_col:
        print(f"   Kolommen niet gevonden. Beschikbaar: {list(df.columns)}")
        context["ti"].xcom_push(
            key="kmi",
            value=pd.DataFrame(columns=["datum", "kmi_radiation_avg"]).to_json(),
        )
        return

    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df["datum"] = df[time_col].dt.normalize()
    df[rad_col] = pd.to_numeric(df[rad_col], errors="coerce")

    daily = df.groupby("datum")[rad_col].mean().reset_index()
    daily.columns = ["datum", "kmi_radiation_avg"]
    daily = daily[daily["datum"] >= START_DATE]

    print(f"   KMI: {len(daily)} dagen opgehaald.")
    context["ti"].xcom_push(key="kmi", value=daily.to_json(date_format="iso"))


def task_fetch_kaggle(**context):
    """Download Kaggle Belgium weather data en bereken daggemiddelde straling (W/m²)."""
    print("Dataset 3: Kaggle Belgium weather straling ophalen...")

    RAW_PATH = "/data/raw"
    DATASET = "chrlkadm/belgium-weather-data-from-two-stations-since-2020"
    CSV_NAME = "aws_1day.csv"

    os.makedirs(RAW_PATH, exist_ok=True)
    csv_path = os.path.join(RAW_PATH, CSV_NAME)

    if not os.path.exists(csv_path):
        if os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY"):
            from kaggle.api.kaggle_api_extended import KaggleApi

            api = KaggleApi()
            api.authenticate()
            api.dataset_download_files(DATASET, path=RAW_PATH, unzip=True)
            print("   Kaggle download voltooid.")
        else:
            print("   Geen Kaggle credentials, skip.")
            context["ti"].xcom_push(
                key="kaggle",
                value=pd.DataFrame(columns=["datum", "kaggle_radiation_avg"]).to_json(),
            )
            return

    if not os.path.exists(csv_path):
        csvs = [f for f in os.listdir(RAW_PATH) if f.endswith(".csv")]
        if csvs:
            csv_path = os.path.join(RAW_PATH, csvs[0])
        else:
            context["ti"].xcom_push(
                key="kaggle",
                value=pd.DataFrame(columns=["datum", "kaggle_radiation_avg"]).to_json(),
            )
            return

    df = pd.read_csv(csv_path)
    df.columns = [c.strip().lower() for c in df.columns]

    time_col = next(
        (c for c in ["timestamp", "time", "datetime", "date"] if c in df.columns), None
    )
    rad_col = next(
        (
            c
            for c in [
                "short_wave_from_sky_avg",
                "short_wave_from_sky",
                "sw_radiation",
                "shortwave_radiation",
                "global_radiation",
                "radiation",
            ]
            if c in df.columns
        ),
        None,
    )

    if not time_col or not rad_col:
        print(f"   Kolommen niet gevonden. Beschikbaar: {list(df.columns)}")
        context["ti"].xcom_push(
            key="kaggle",
            value=pd.DataFrame(columns=["datum", "kaggle_radiation_avg"]).to_json(),
        )
        return

    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df["datum"] = df[time_col].dt.normalize()
    df[rad_col] = pd.to_numeric(df[rad_col], errors="coerce")

    daily = df.groupby("datum")[rad_col].mean().reset_index()
    daily.columns = ["datum", "kaggle_radiation_avg"]
    daily = daily[daily["datum"] >= START_DATE]

    print(f"   Kaggle: {len(daily)} dagen opgehaald.")
    context["ti"].xcom_push(key="kaggle", value=daily.to_json(date_format="iso"))


def task_combine_and_load(**context):
    """Combineer alle datasets en schrijf naar PostgreSQL."""
    print("Datasets combineren en laden naar database...")

    ti = context["ti"]

    # Haal data op uit XCom
    df_open_meteo = pd.read_json(
        ti.xcom_pull(task_ids="fetch_open_meteo", key="open_meteo")
    )
    df_kmi = pd.read_json(ti.xcom_pull(task_ids="fetch_kmi", key="kmi"))
    df_kaggle = pd.read_json(ti.xcom_pull(task_ids="fetch_kaggle", key="kaggle"))

    # Normaliseer datum kolommen
    for df in [df_open_meteo, df_kmi, df_kaggle]:
        if "datum" in df.columns:
            df["datum"] = pd.to_datetime(df["datum"]).dt.normalize()

    # Datumreeks van 2020-01-01 tot vandaag
    dates = pd.DataFrame(
        {"datum": pd.date_range(start=START_DATE, end=date.today(), freq="D")}
    )

    # Combineer via left join
    combined = dates.merge(df_open_meteo, on="datum", how="left")
    combined = combined.merge(df_kmi, on="datum", how="left")
    combined = combined.merge(df_kaggle, on="datum", how="left")

    # ID kolom
    combined.insert(0, "id", range(1, len(combined) + 1))

    print(f"Resultaat: {len(combined)} rijen, kolommen: {list(combined.columns)}")
    print(combined.head(10))

    # Schrijf naar PostgreSQL
    engine = get_engine()
    combined.to_sql("combined_weather", engine, if_exists="replace", index=False)
    print(f"Succes! Tabel 'combined_weather' aangemaakt met {len(combined)} rijen.")


# === DAG definitie ===
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="combined_weather_pipeline",
    default_args=default_args,
    description="Combineer straling data van Open-Meteo, KMI en Kaggle",
    start_date=datetime(2020, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["weather", "radiation"],
) as dag:

    fetch_open_meteo = PythonOperator(
        task_id="fetch_open_meteo",
        python_callable=task_fetch_open_meteo,
    )

    fetch_kmi = PythonOperator(
        task_id="fetch_kmi",
        python_callable=task_fetch_kmi,
    )

    fetch_kaggle = PythonOperator(
        task_id="fetch_kaggle",
        python_callable=task_fetch_kaggle,
    )

    combine_and_load = PythonOperator(
        task_id="combine_and_load",
        python_callable=task_combine_and_load,
    )

    # Alle 3 fetch tasks parallel, daarna combineren
    [fetch_open_meteo, fetch_kmi, fetch_kaggle] >> combine_and_load
