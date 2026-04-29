# combineren — Wind, Zon, Energieproductie & Consumptie

> Project: `combineren`
> Auteur: Stijn Voeten (AP Hogeschool Antwerpen, Data Engineering 2025-2026)
> Laatst bijgewerkt: 2026-04-29

Verzamelen, beschrijven, transformeren en orkestreren van ruwe brondata over weer, hernieuwbare productie en elektriciteitsverbruik in België/Vlaanderen tot één coherente analytische dataset, **met als einddoel het voorspellen van de hernieuwbare energieproductie (zon + wind) op basis van een weersvoorspelling**.

> Voor het volledige metadata-/data-dictionary van de datasets, zie [DATA_DOCUMENTATION.md](DATA_DOCUMENTATION.md).

---

## 1. Bredere context

### 1.1 Projectdoel — voorspellen van hernieuwbare productie op basis van weer

Het concrete doel van dit project is **het voorspellen van de opgewekte hernieuwbare energie (zon en wind) aan de hand van een weersvoorspelling**. De ETL-pipeline en het sterschema zijn daar volledig op gericht: we verzamelen de meteo-variabelen die fysisch bepalend zijn voor de productie (kortgolvige straling voor PV, windsnelheid voor windturbines) **samen** met de werkelijke gemeten productie van Elia/VEKA, zodat een model het verband tussen beide kan leren.

De gedocumenteerde relaties tussen meteo-kolommen en productie-kolommen (zie [DATA_DOCUMENTATION.md §2](DATA_DOCUMENTATION.md#2-relaties-tussen-variabelen)) zijn tegelijk **kandidaat-features** voor het voorspellingsmodel. Het kruisvalideren van meerdere bronnen voor dezelfde grootheid (vier windbronnen, drie stralingsbronnen) is geen overdaad maar dient om de **robuustheid** van die features tegen ontbrekende of biased meetstations te garanderen.

### 1.2 Energie-context

België haalt ondertussen een groot deel van zijn elektriciteit uit hernieuwbare bronnen. De netbeheerder **Elia** publiceert hiervoor open data via **Open Data Portal Elia** (https://opendata.elia.be). Twee dominante hernieuwbare bronnen zijn:

- **Zon (PV):** sterk afhankelijk van **kortgolvige straling** (W/m²), zonnehoek, bewolking en seizoen. Productie volgt een dagcyclus (0 's nachts) en piekt rond zonnestand.
- **Wind:** sterk afhankelijk van **windsnelheid** op masthoogte. Vlaanderen heeft offshore (Noordzee) en onshore parken; offshore is dominant qua geïnstalleerd vermogen sinds ~2020.

Aan de **vraagzijde** (consumptie) speelt seizoenaliteit (verwarming/koeling), uur-van-de-dag (ochtend- en avondpiek) en in toenemende mate **prosumer-gedrag** (zelfconsumptie, batterijen, EV's).

De keuze om **meerdere bronnen voor hetzelfde fenomeen** te combineren (bijv. wind via ECMWF, KMI, Ukkel én Antwerpen-archief) is bewust: het laat toe om **gaps op te vullen**, **bronnen te kruisvalideren**, en de gevoeligheid van een ETL-pipeline aan ontbrekende bronnen te demonstreren.

### 1.3 Conceptueel model

```
   STRALING (W/m²)            WIND (m/s)
        │                         │
        ▼                         ▼
  PV-productie ───┐         Wind-productie
  (zon_kwh)       │              (wind_kwh)
                  ▼
           ┌─────────────┐
           │  PRODUCTIE  │
           └──────┬──────┘
                  │ injectie op net
                  ▼
            ┌──────────┐
            │  ELIA    │ ◀── totaalverbruik (consumptie)
            │  GRID    │
            └──────────┘
                  ▲
                  │ afname
        ┌─────────┴─────────┐
   privé verbruik     openbaar verbruik
```

---

## 2. Opslag van de ruwe data

Ruwe bestanden blijven **immutable** bewaard in:

```
combineren/
├── datasets/                       # gecommitte CSV-snapshots (handmatige downloads)
│   ├── consumptie.csv
│   ├── productie_comnbined.csv
│   └── v_wind_alles_compleet.csv
└── (volume in container)
    └── /data/raw/                  # door Airflow gedownloade Kaggle/API-payloads
```

In `docker-compose.yml` is een named volume `kaggle_data` gemount op `/data/raw`, en de map `datasets/` is gemount op `/data/datasets`. Dat scheidt **handmatig aangeleverde** snapshots van **automatisch gedownloade** ruwe payloads.

---

## 3. Doelstructuur

### 3.1 Modelleringskeuze

Een **eenvoudig sterschema** met één feittabel per uur en aparte dimensietabellen voor tijd en bron. Reden: de analytische vragen (correlatie wind↔productie, vraag↔aanbod, capaciteitsfactor per bron) zijn allemaal tijdseries-georiënteerd.

```
                ┌────────────────┐
                │  dim_tijd      │ (uur, dag, maand, seizoen, weekday)
                └───────┬────────┘
                        │
   ┌────────────────────┼────────────────────┐
   ▼                    ▼                    ▼
fact_wind_obs    fact_productie         fact_consumptie
(per uur,        (zon_kwh,              (elia_totaal,
 4 bronnen       wind_kwh per           privé,
 in long form)   regio in long form)    openbaar)
                        │
                        ▼
                  fact_straling
                  (open_meteo, kmi, kaggle)
```

Wide-tabellen zoals `consumptie.csv` worden **gepivoteerd naar long-format** (`tijd, bron, waarde`), wat het toevoegen van een nieuwe bron triviaal maakt en analyses in Grafana eenvoudiger.

### 3.2 Doel-database: PostgreSQL 16

Reeds in `docker-compose.yml` aanwezig (`db`-service, database `combined_weather`). Argumenten:

- vrij, productiewaardig, ondersteunt `TIMESTAMPTZ` correct (kritiek voor onze UTC/lokaal-mix);
- `pgAdmin` en `Grafana` zijn al gemount in de stack;
- relationeel model past natuurlijk bij sterschema;
- voor tijdseries-werk kan later `TimescaleDB`-extensie worden toegevoegd zonder schema-breuk.

---

## 4. ETL-proces

### 4.1 Extract

| Bron | Wijze | Frequentie |
|---|---|---|
| Open-Meteo straling/wind | HTTPS GET (JSON) | dagelijks, archief tot vandaag-5d |
| KMI AWS | WFS GetFeature (CSV) | dagelijks |
| Kaggle dataset | Kaggle API + lokale cache in `/data/raw` | maandelijks |
| Elia / VEKA CSV-snapshots | manueel naar `datasets/` | ad hoc |

### 4.2 Transform

1. **Parseren** van `tijd`/`tijdstip` met `pd.to_datetime(..., utc=True)`.
2. **Tijdzone normaliseren** naar UTC; `consumptie.csv` (lokale tijd) wordt expliciet gelokaliseerd op `Europe/Brussels` met DST-handling.
3. **Lege strings en `"NULL"` herinterpreteren** als `NaN`.
4. **Uniformeren van eenheden** — kWh blijft kWh, MWh wordt geconverteerd naar kWh waar nodig (nog te beslissen op basis van bronvalidatie).
5. **Dedupliceren** op `(tijd, bron)`.
6. **Validatie**: monotone tijdreeks zonder gaten >1 dag, anders flag in `etl_quality_log`.
7. **Pivoteren** van wide-csv's naar long-format voor de feittabellen.

### 4.3 Load

`pandas.to_sql(..., method="multi", if_exists="append")` op staging-tabellen, daarna `INSERT ... ON CONFLICT DO UPDATE` (upsert) naar de definitieve fact-tabellen op de natural key `(tijd, bron)`. De huidige `init.sql` bevat al `combined_weather` als feittabel voor straling — dat schema wordt uitgebreid met `fact_wind_obs`, `fact_productie`, `fact_consumptie`, `dim_tijd`.

---

## 5. Orkestratie in Airflow

De Airflow-stack is geconfigureerd in `docker-compose.yml` (services `airflow-init`, `airflow-webserver`, `airflow-scheduler`, executor `LocalExecutor`). Bestaande DAGs:

- [`combined_weather_pipeline`](dags/combined_weather_pipeline.py) — straling van 3 bronnen, parallel, daarna combineren en laden.
- [`csv_pipelines.py`](dags/csv_pipelines.py) — genereert dynamisch één DAG per CSV in `datasets/`.

### 5.1 Geplande uitbreiding — DAG `energy_master_pipeline`

```
       ┌─────────────────┐
       │  dim_tijd_seed  │   (één keer)
       └────────┬────────┘
                │
   ┌────────────┼────────────┬───────────────┐
   ▼            ▼            ▼               ▼
fetch_wind  fetch_radiation fetch_prod   fetch_cons
 (4 bronnen) (3 bronnen)    (Elia+VEKA)  (CSV/Kaggle)
   │            │            │              │
   └────────────┼────────────┼──────────────┘
                ▼            ▼
            transform_long  validate_quality
                 │              │
                 └──────┬───────┘
                        ▼
                  upsert_to_postgres
                        │
                        ▼
                  refresh_grafana_views
```

- **Schedule:** `@daily` om 04:00 UTC (na publicatie van Elia/KMI van de vorige dag).
- **Retries:** 2 met exponentiële back-off; **catchup=False** want het archief wordt apart geladen.
- **XCom**: alleen kleine controle-payloads — grote DataFrames worden via een gedeeld volume of staging-tabel doorgegeven om serialisatie-issues met `TIMESTAMPTZ` te vermijden (zoals reeds in `csv_pipelines.py`).
- **Monitoring:** Airflow UI op `:8080`, dataset-kwaliteit visueel in **Grafana** op `:3000` met PostgreSQL-datasource.

---

## 6. Data catalog

De metadata uit [DATA_DOCUMENTATION.md](DATA_DOCUMENTATION.md) wordt ook **als data** in dezelfde Postgres geladen, zodat de catalog opvraagbaar is via SQL en visualiseerbaar in Grafana / pgAdmin.

**Schema:**

| Tabel | Inhoud |
|---|---|
| `metadata_dataset` | één rij per dataset (naam, bron, periode, encoding, licentie, …) |
| `metadata_column` | één rij per kolom met type, eenheid en betekenis |
| `metadata_relation` | gerichte relaties tussen variabelen (bv. wind → windproductie) |

DDL staat in [init.sql](init.sql) en wordt ook idempotent door de DAG zelf gegarandeerd.

**Pipeline:** [`dags/metadata_pipeline.py`](dags/metadata_pipeline.py) — DAG `metadata_pipeline`:

```
create_schema → upsert_datasets ─┬─→ upsert_columns
                                 └─→ upsert_relations
```

- **Schedule:** `None` (manuele trigger of via `airflow dags trigger metadata_pipeline`); metadata verandert weinig.
- **Idempotent**: `INSERT ... ON CONFLICT DO UPDATE` op natural keys (`name`, `(dataset_name, name)`, `(source_dataset, source_column, target_dataset, target_column)`).
- Geen XCom: alle definities zijn Python-constanten in dezelfde module, één engine per task.

Voorbeeld-query's na een run:

```sql
SELECT name, period_start, period_end, row_count FROM metadata_dataset;
SELECT * FROM metadata_column WHERE dataset_name = 'productie_comnbined';
SELECT * FROM metadata_relation WHERE relation_type = 'drives';
```

### Visualisatie in Grafana

Grafana wordt automatisch geprovisioneerd via [grafana/provisioning/](grafana/provisioning/):

- **Datasource** `combined_weather` (PostgreSQL, uid `combined_weather_pg`) — [postgres.yml](grafana/provisioning/datasources/postgres.yml)
- **Dashboard** "Data Catalog" — [data_catalog.json](grafana/dashboards/data_catalog.json)

Dashboard-panelen:

| Panel | Inhoud |
|---|---|
| Stats × 3 | aantal datasets / kolommen / relaties |
| Tabel "Datasets" | naam, bron, periode, granulariteit, licentie, rijen |
| Tabel "Kolommen ($dataset)" | filterbaar via dropdown-variabele |
| Tabel "Relaties" | gerichte links tussen variabelen |
| Bar chart | aantal kolommen per dataset |
| Tabel "Looptijd" | periode in dagen per dataset |

Bekijk via http://localhost:3000 (admin/admin) na `docker compose up -d` en een trigger van `metadata_pipeline`.

---

## 7. Open punten / next steps

- [ ] Bronvalidatie van eenheden (MWh vs kWh) voor `consumptie.csv` kolommen Vlaanderen.
- [ ] Schema voor `fact_productie`, `fact_consumptie`, `fact_wind_obs`, `dim_tijd` toevoegen aan `init.sql`.
- [ ] DAG `energy_master_pipeline` schrijven die de aparte CSV-DAGs opvolgt.
- [ ] Grafana dashboard "Hernieuwbaar vs verbruik" met capaciteitsfactor per bron.
- [ ] Data-quality DAG die gaps en outliers per bron detecteert.
