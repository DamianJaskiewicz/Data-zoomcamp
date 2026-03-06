#!/usr/bin/env python3
"""
Ingest NYC TLC Yellow Taxi data into Postgres (chunked).

Założenia zgodne z Twoim notebookiem:
- Postgres: root/root@localhost:5432
- DB: ny_taxi
- Table: yellow_taxi_data
- Źródło: GitHub DataTalksClub (pliki .csv.gz)
"""

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


def main() -> None:
    # --- Parametry datasetu (jak w notebooku) ---
    year = 2021
    month = 1

    # --- Budowa URL (POPRAWKA: bez dodatkowej "1") ---
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"

    # --- Typy kolumn: wymuszamy schemat (to jest kluczowe) ---
    dtype = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "RatecodeID": "Int64",
        "store_and_fwd_flag": "string",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64",
    }

    # --- Kolumny dat: parsujemy na datetime ---
    parse_dates = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
    ]

    # --- Dane do bazy: zgodnie z tym co podałeś ---
    pg_user = "root"
    pg_pass = "root"
    pg_host = "localhost"
    pg_port = 5432
    pg_db = "ny_taxi"
    table_name = "yellow_taxi_data"

    # --- Tworzymy engine SQLAlchemy (psycopg3) ---
    engine = create_engine(
        f"postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    # --- Czytamy plik chunkami (żeby nie zajechać RAM-u) ---
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=100_000,
        low_memory=False,
    )

    # --- Pierwszy chunk tworzy tabelę, kolejne dopisują dane ---
    first = True
    total = 0

    for df_chunk in tqdm(df_iter, desc="Loading chunks"):
        if first:
            # Tworzymy tabelę w Postgresie na podstawie schematu chunk-a, bez danych
            df_chunk.head(0).to_sql(
                name=table_name,
                con=engine,
                if_exists="replace",
                index=False,
            )
            first = False
            print("[INFO] Table created (schema).")

        # Wrzucamy dane (append)
        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10_000,
        )

        total += len(df_chunk)

    print(f"[DONE] Inserted rows: {total}")


if __name__ == "__main__":
    main()