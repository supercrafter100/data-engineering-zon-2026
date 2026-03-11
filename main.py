import pandas as pd
import requests
import io
import os
import time
from datetime import date, timedelta
from sqlalchemy import create_engine, text

# === Database configuratie ===
DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "combined_weather")
DB_USER = os.getenv("POSTGRES_USER", "admin")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "password")
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

START_DATE = "2020-01-01"
LATITUDE = 51.2205  # Antwerpen
LONGITUDE = 4.4003


def wait_for_db():
    """Wacht tot de database beschikbaar is en geef engine terug."""
    while True:
        try:
            engine = create_engine(DB_URL)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Database verbinding OK.")
            return engine
        except Exception:
            print("Database nog niet klaar... 3s wachten.")
            time.sleep(3)


def fetch_open_meteo() -> pd.DataFrame:
    """Haal historische uurlijkse straling op van Open-Meteo Archive API en bereken daggemiddelde (W/m²)."""
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

    try:
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
        return daily
    except Exception as e:
        print(f"   Fout bij Open-Meteo: {e}")
        return pd.DataFrame(columns=["datum", "open_meteo_radiation"])


def fetch_kmi() -> pd.DataFrame:
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

    try:
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()

        df = pd.read_csv(io.BytesIO(resp.content), encoding="ISO-8859-1")
        df.columns = [
            c.split(":")[-1].replace('"', "").strip().lower() for c in df.columns
        ]

        # Zoek timestamp kolom
        time_col = None
        for col in ["timestamp", "time", "datetime", "date"]:
            if col in df.columns:
                time_col = col
                break

        if time_col is None:
            print(
                f"   Geen tijdskolom gevonden in KMI data. Kolommen: {list(df.columns)}"
            )
            return pd.DataFrame(columns=["datum", "kmi_radiation_avg"])

        # Zoek straling kolom (shortwave/global radiation in W/m²)
        rad_col = None
        for col in [
            "short_wave_from_sky_avg",
            "short_wave_from_sky",
            "sw_radiation",
            "shortwave_radiation",
            "global_radiation",
            "radiation",
            "rad",
            "solarrad",
        ]:
            if col in df.columns:
                rad_col = col
                break

        if rad_col is None:
            print(
                f"   Geen stralingskolom gevonden in KMI data. Kolommen: {list(df.columns)}"
            )
            return pd.DataFrame(columns=["datum", "kmi_radiation_avg"])

        df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
        df["datum"] = df[time_col].dt.normalize()
        df[rad_col] = pd.to_numeric(df[rad_col], errors="coerce")

        daily = df.groupby("datum")[rad_col].mean().reset_index()
        daily.columns = ["datum", "kmi_radiation_avg"]

        daily = daily[daily["datum"] >= START_DATE]

        print(f"   KMI: {len(daily)} dagen opgehaald.")
        return daily
    except Exception as e:
        print(f"   Fout bij KMI: {e}")
        return pd.DataFrame(columns=["datum", "kmi_radiation_avg"])


def fetch_kaggle() -> pd.DataFrame:
    """Download Kaggle Belgium weather data en bereken daggemiddelde straling (W/m²)."""
    print("Dataset 3: Kaggle Belgium weather straling ophalen...")

    RAW_PATH = "/data/raw"
    DATASET = "chrlkadm/belgium-weather-data-from-two-stations-since-2020"
    CSV_NAME = "aws_1day.csv"

    os.makedirs(RAW_PATH, exist_ok=True)
    csv_path = os.path.join(RAW_PATH, CSV_NAME)

    # Download via Kaggle API als bestand nog niet bestaat
    if not os.path.exists(csv_path):
        if os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY"):
            try:
                from kaggle.api.kaggle_api_extended import KaggleApi

                api = KaggleApi()
                api.authenticate()
                api.dataset_download_files(DATASET, path=RAW_PATH, unzip=True)
                print("   Kaggle download voltooid.")
            except Exception as e:
                print(f"   Kaggle download fout: {e}")
                return pd.DataFrame(columns=["datum", "kaggle_radiation_avg"])
        else:
            print("   Geen Kaggle credentials gevonden (KAGGLE_USERNAME / KAGGLE_KEY).")
            return pd.DataFrame(columns=["datum", "kaggle_radiation_avg"])

    # Zoek CSV bestand
    if not os.path.exists(csv_path):
        csvs = [f for f in os.listdir(RAW_PATH) if f.endswith(".csv")]
        if csvs:
            csv_path = os.path.join(RAW_PATH, csvs[0])
        else:
            print("   Geen CSV gevonden na download.")
            return pd.DataFrame(columns=["datum", "kaggle_radiation_avg"])

    try:
        df = pd.read_csv(csv_path)
        df.columns = [c.strip().lower() for c in df.columns]

        # Zoek timestamp kolom
        time_col = None
        for col in ["timestamp", "time", "datetime", "date"]:
            if col in df.columns:
                time_col = col
                break

        if time_col is None:
            print(
                f"   Geen tijdskolom gevonden in Kaggle data. Kolommen: {list(df.columns)}"
            )
            return pd.DataFrame(columns=["datum", "kaggle_radiation_avg"])

        # Zoek straling kolom (shortwave/global radiation in W/m²)
        rad_col = None
        for col in [
            "short_wave_from_sky_avg",
            "short_wave_from_sky",
            "sw_radiation",
            "shortwave_radiation",
            "global_radiation",
            "radiation",
            "rad",
            "solarrad",
        ]:
            if col in df.columns:
                rad_col = col
                break

        if rad_col is None:
            print(
                f"   Geen stralingskolom gevonden in Kaggle data. Kolommen: {list(df.columns)}"
            )
            return pd.DataFrame(columns=["datum", "kaggle_radiation_avg"])

        df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
        df["datum"] = df[time_col].dt.normalize()
        df[rad_col] = pd.to_numeric(df[rad_col], errors="coerce")

        # Gemiddelde per dag (over alle stations)
        daily = df.groupby("datum")[rad_col].mean().reset_index()
        daily.columns = ["datum", "kaggle_radiation_avg"]

        daily = daily[daily["datum"] >= START_DATE]

        print(f"   Kaggle: {len(daily)} dagen opgehaald.")
        return daily
    except Exception as e:
        print(f"   Fout bij Kaggle data: {e}")
        return pd.DataFrame(columns=["datum", "kaggle_radiation_avg"])


def main():
    print("=" * 50)
    print("  Gecombineerde Weer Data Pipeline")
    print(f"  Periode: {START_DATE} t/m {date.today()}")
    print("=" * 50)

    # Maak datumreeks van 2020-01-01 tot vandaag
    dates = pd.DataFrame(
        {"datum": pd.date_range(start=START_DATE, end=date.today(), freq="D")}
    )

    # Haal data op van alle 3 bronnen
    df_open_meteo = fetch_open_meteo()
    df_kmi = fetch_kmi()
    df_kaggle = fetch_kaggle()

    # Combineer via left join op datum (NULL als er geen data is)
    combined = dates.merge(df_open_meteo, on="datum", how="left")
    combined = combined.merge(df_kmi, on="datum", how="left")
    combined = combined.merge(df_kaggle, on="datum", how="left")

    # Voeg ID kolom toe
    combined.insert(0, "id", range(1, len(combined) + 1))

    print(f"\nResultaat: {len(combined)} rijen, kolommen: {list(combined.columns)}")
    print(combined.head(10))
    print("...")
    print(combined.tail(5))

    # Schrijf naar PostgreSQL
    engine = wait_for_db()
    combined.to_sql("combined_weather", engine, if_exists="replace", index=False)
    print(f"\nSucces! Tabel 'combined_weather' aangemaakt met {len(combined)} rijen.")


if __name__ == "__main__":
    main()
