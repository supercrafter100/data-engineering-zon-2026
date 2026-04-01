import argparse
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine


def build_db_url() -> str:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "combined_weather")
    user = os.getenv("POSTGRES_USER", "admin")
    password = os.getenv("POSTGRES_PASSWORD", "password")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def find_date_column(columns: list[str]) -> str:
    lookup = {c.strip().lower(): c for c in columns}
    for candidate in ("tijd", "tijdstip"):
        if candidate in lookup:
            return lookup[candidate]
    raise ValueError("Geen datumkolom gevonden. Verwacht kolom 'tijd' of 'tijdstip'.")


def import_csv_file(csv_path: Path, engine, if_exists: str) -> None:
    table_name = csv_path.stem
    df = pd.read_csv(csv_path)

    date_col = find_date_column(df.columns.tolist())
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # Drop rows where date parsing failed to avoid invalid timestamps in DB.
    df = df.dropna(subset=[date_col])

    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    print(f"{csv_path.name} -> tabel '{table_name}' ({len(df)} rijen)")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Importeer alle CSV-bestanden uit een map naar PostgreSQL."
    )
    parser.add_argument(
        "--folder",
        default="datasets",
        help="Map met CSV-bestanden (standaard: datasets)",
    )
    parser.add_argument(
        "--if-exists",
        choices=["replace", "append", "fail"],
        default="replace",
        help="Gedrag als tabel al bestaat (standaard: replace)",
    )
    args = parser.parse_args()

    folder = Path(args.folder)
    if not folder.exists() or not folder.is_dir():
        print(f"Map niet gevonden: {folder}")
        return 1

    csv_files = sorted(folder.glob("*.csv"))
    if not csv_files:
        print(f"Geen CSV-bestanden gevonden in {folder}")
        return 1

    engine = create_engine(build_db_url(), pool_pre_ping=True)

    for csv_file in csv_files:
        try:
            import_csv_file(csv_file, engine, args.if_exists)
        except Exception as exc:
            print(f"Fout bij importeren van {csv_file.name}: {exc}")
            return 1

    print("Klaar. Alle CSV-bestanden zijn geimporteerd.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
