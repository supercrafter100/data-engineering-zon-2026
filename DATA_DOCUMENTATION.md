# Data documentatie — Wind, Zon, Energieproductie & Consumptie

> Project: `combineren`
> Auteur: Stijn Voeten (AP Hogeschool Antwerpen, Data Engineering 2025-2026)
> Laatst bijgewerkt: 2026-04-29
> Doel: alle ruwe brondata over weer, hernieuwbare productie en elektriciteitsverbruik in België/Vlaanderen verzamelen, beschrijven, transformeren en orkestreren tot één coherente analytische dataset.

---

## 1. Bredere context (contextual embedding)

België haalt ondertussen een groot deel van zijn elektriciteit uit hernieuwbare bronnen. De netbeheerder **Elia** publiceert hiervoor open data via **Open Data Portal Elia** (https://opendata.elia.be). Twee dominante hernieuwbare bronnen zijn:

- **Zon (PV):** sterk afhankelijk van **kortgolvige straling** (W/m²), zonnehoek, bewolking en seizoen. Productie volgt een dagcyclus (0 's nachts) en piekt rond zonnestand.
- **Wind:** sterk afhankelijk van **windsnelheid** op masthoogte. Vlaanderen heeft offshore (Noordzee) en onshore parken; offshore is dominant qua geïnstalleerd vermogen sinds ~2020.

Aan de **vraagzijde** (consumptie) speelt seizoenaliteit (verwarming/koeling), uur-van-de-dag (ochtend- en avondpiek) en in toenemende mate **prosumer-gedrag** (zelfconsumptie, batterijen, EV's).

Bronnen die door dit project gebruikt worden:

| Bron | Gebruikt voor | Type |
|---|---|---|
| **Elia Open Data** | productie zon/wind (`elia_zon_kwh`, `elia_wind_kwh`), totaalverbruik | officieel, netbeheerder |
| **VEKA / Vlaanderen Energie- en Klimaatagentschap** | regionale productie Vlaanderen | overheidsstatistiek |
| **KMI (meteo.be) AWS** | meteo-observaties (straling, wind) | officiële meteo |
| **Open-Meteo Archive API** | reanalysedata wind/straling | open API, wereldwijd |
| **ECMWF (via Open-Meteo)** | windsnelheid model | reanalyse/forecast |
| **Kaggle dataset Belgium weather** | tweede-hands meteo, kruisvalidatie | community |

De keuze om **meerdere bronnen voor hetzelfde fenomeen** te combineren (bijv. wind via ECMWF, KMI, Ukkel én Antwerpen-archief) is bewust: het laat toe om **gaps op te vullen**, **bronnen te kruisvalideren**, en de gevoeligheid van een ETL-pipeline aan ontbrekende bronnen te demonstreren.

---

## 2. Datasets — schema en semantiek

Alle CSV's bevinden zich in `datasets/`. Encoding: **UTF-8**. Scheiding: **komma**. Decimaal: **punt**. Timestamps zijn uurgranulair, in UTC tenzij anders vermeld.

### 2.1 `consumptie.csv` — Elektriciteitsverbruik

| Eigenschap | Waarde |
|---|---|
| Onderwerp | Uurlijks elektriciteitsverbruik in Vlaanderen / België |
| Granulariteit | 1 uur |
| Periode | 2021-01-01 00:00 → 2026-02-10 23:00 |
| Aantal rijen | ~17.759 |
| Encoding | UTF-8 |
| Tijdzone | lokale tijd (Europe/Brussels), kolom `tijd` zonder offset |

**Kolommen:**

| Kolom | Type | Eenheid | Betekenis |
|---|---|---|---|
| `tijd` | timestamp | — | Uurstempel (lokale tijd) |
| `Energie vlaanderen zon` | float | MWh of GWh | Door zon opgewekt vermogen Vlaanderen (regionaal) |
| `Energie vlaanderen wind` | float | MWh of GWh | Door wind opgewekt vermogen Vlaanderen |
| `Elia totaal` | float | MWh | Totaal elektriciteitsverbruik op het Elia-net |
| `kaggle prive` | float | MWh | Verbruik privé-aansluitingen (Kaggle-bron) |
| `kaggle openbaar` | float | MWh | Verbruik openbare aansluitingen (Kaggle-bron) |

> Opmerking: de eerste vier kolommen bevatten leemtes vóór 2024 — de Vlaanderen-cijfers en Elia-totaal zijn pas later in de reeks beschikbaar. Dit is **geen fout** maar reflecteert de start van publicatie van die feeds.

### 2.2 `productie_comnbined.csv` — Hernieuwbare productie

| Eigenschap | Waarde |
|---|---|
| Onderwerp | Geïnjecteerde productie zon/wind (Vlaanderen + Elia) |
| Granulariteit | 1 uur |
| Periode | 2025-02-28 23:00 → 2026-03-25 22:00 (UTC) |
| Aantal rijen | ~9.361 |
| Encoding | UTF-8 |
| Tijdzone | UTC (`+00` suffix) |
| `NULL` markering | letterlijk de string `NULL` |

**Kolommen:**

| Kolom | Type | Eenheid | Betekenis |
|---|---|---|---|
| `tijd` | timestamptz | — | Uurstempel UTC |
| `vlaanderen_zon_kwh` | int | kWh | PV-productie Vlaanderen (VEKA-feed) |
| `vlaanderen_wind_kwh` | int | kWh | Windproductie Vlaanderen |
| `elia_zon_kwh` | int | kWh | PV-productie via Elia-rapportering (België) |
| `elia_wind_kwh` | int | kWh | Wind-productie via Elia-rapportering (België) |

> De Elia-kolommen zijn pas vanaf 2025-03-01 ingevuld; eerdere rijen staan op `NULL`. De Vlaanderen-feed loopt door tot maart 2026.

### 2.3 `v_wind_alles_compleet.csv` — Windsnelheidsobservaties

| Eigenschap | Waarde |
|---|---|
| Onderwerp | Windsnelheid uit vier bronnen, gestapeld per uur |
| Granulariteit | 1 uur |
| Periode | 2005-11-10 → 2026-03-24 (UTC) |
| Aantal rijen | ~1.137.676 |
| Encoding | UTF-8 |
| Eenheid | m/s (op 10 m hoogte, te verifiëren per bron) |
| `NULL` markering | letterlijk `NULL` |

**Kolommen:**

| Kolom | Type | Bron | Periode actief |
|---|---|---|---|
| `tijdstip` | timestamptz | — | 2005-… |
| `wind_ecmwf_2026` | float | ECMWF reanalyse via Open-Meteo | 2024+ (reanalyse t.b.v. recent) |
| `wind_kmi_2002` | float | KMI archief (Brussels/Ukkel) | sinds 2002 |
| `wind_ukkel_2024` | float | KMI station Ukkel (recent) | 2024+ |
| `wind_antwerpen_archive` | float | Archief station Antwerpen (Deurne) | historisch |

> De naamgeving van de kolommen verwijst naar de bron én het startjaar van de reeks. Het is bewust een **brede, sparse tabel**: per uur staat doorgaans maar één van de vier bronnen ingevuld.

---

## 3. Relaties tussen variabelen

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

**Belangrijkste verwachte correlaties:**

1. **`v_wind_alles_compleet.wind_*` ↔ `productie_comnbined.vlaanderen_wind_kwh`**
   Hogere windsnelheid → hogere windproductie, met een **kubische** afhankelijkheid (P ∝ v³) tot de cut-off snelheid van de turbines.
2. **Straling (`combined_weather.*_radiation`) ↔ `productie_comnbined.vlaanderen_zon_kwh`**
   Quasi-lineair tijdens daglicht; 's nachts beide nul.
3. **`consumptie.Elia totaal` ↔ uur van de dag / seizoen**
   Sterke dagelijkse en jaarlijkse seizoenaliteit.
4. **`productie - consumptie`** (afgeleide variabele): netto invoer/uitvoer België — interessant voor analyse van **negatieve prijzen** en momenten van **curtailment**.
5. **Tussen wind-bronnen onderling**: hoge correlatie verwacht maar met **bias** per station (locatieverschil Antwerpen vs Ukkel) — bruikbaar voor data-quality checks.

**Join-sleutel:** in alle datasets is dat het **uurstempel**. Voor heterogene tijdzones (`consumptie.csv` lokaal vs. `productie/wind` UTC) moet bij het joinen genormaliseerd worden naar UTC.

---

## 4. Aanvullende metadata

| Aspect | Waarde |
|---|---|
| Talen in data | Nederlands (kolomnamen), getallen in EN-formaat |
| Bestandsformaat | CSV |
| Compressie | geen |
| Licenties bronnen | Elia: open (CC-BY), KMI Open Data: vrij gebruik met bronvermelding, Open-Meteo: CC-BY-4.0, Kaggle: per dataset (community) |
| Privacy | geen persoonsgegevens; alle data is geaggregeerd op regio of station |
| Dataverversing | Elia/KMI: dagelijks; Open-Meteo archive: ~5 dagen vertraging |
| Versie van deze export | snapshot van april 2026 |

---

## 5. Opslag van de ruwe data

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

## 6. Doelstructuur — gekozen aanpak

### 6.1 Modelleringskeuze

Ik kies voor een **eenvoudig sterschema** met één feittabel per uur en aparte dimensietabellen voor tijd en bron. Reden: de analytische vragen (correlatie wind↔productie, vraag↔aanbod, capaciteitsfactor per bron) zijn allemaal tijdseries-georiënteerd.

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

### 6.2 Doel-database: **PostgreSQL 16**

Reeds in `docker-compose.yml` aanwezig (`db`-service, database `combined_weather`). Argumenten:

- vrij, productiewaardig, ondersteunt `TIMESTAMPTZ` correct (kritiek voor onze UTC/lokaal-mix);
- `pgAdmin` en `Grafana` zijn al gemount in de stack;
- relationeel model past natuurlijk bij sterschema;
- voor tijdseries-werk kan later `TimescaleDB`-extensie worden toegevoegd zonder schema-breuk.

---

## 7. ETL-proces

### 7.1 Extract

| Bron | Wijze | Frequentie |
|---|---|---|
| Open-Meteo straling/wind | HTTPS GET (JSON) | dagelijks, archief tot vandaag-5d |
| KMI AWS | WFS GetFeature (CSV) | dagelijks |
| Kaggle dataset | Kaggle API + lokale cache in `/data/raw` | maandelijks |
| Elia / VEKA CSV-snapshots | manueel naar `datasets/` | ad hoc |

### 7.2 Transform

1. **Parseren** van `tijd`/`tijdstip` met `pd.to_datetime(..., utc=True)`.
2. **Tijdzone normaliseren** naar UTC; `consumptie.csv` (lokale tijd) wordt expliciet gelokaliseerd op `Europe/Brussels` met DST-handling.
3. **Lege strings en `"NULL"` herinterpreteren** als `NaN`.
4. **Uniformeren van eenheden** — kWh blijft kWh, MWh wordt geconverteerd naar kWh waar nodig (nog te beslissen op basis van bronvalidatie).
5. **Dedupliceren** op `(tijd, bron)`.
6. **Validatie**: monotone tijdreeks zonder gaten >1 dag, anders flag in `etl_quality_log`.
7. **Pivoteren** van wide-csv's naar long-format voor de feittabellen.

### 7.3 Load

`pandas.to_sql(..., method="multi", if_exists="append")` op staging-tabellen, daarna `INSERT ... ON CONFLICT DO UPDATE` (upsert) naar de definitieve fact-tabellen op de natural key `(tijd, bron)`. De huidige `init.sql` bevat al `combined_weather` als feittabel voor straling — dat schema wordt uitgebreid met `fact_wind_obs`, `fact_productie`, `fact_consumptie`, `dim_tijd`.

---

## 8. Orkestratie in Airflow

De Airflow-stack is al geconfigureerd in `docker-compose.yml` (services `airflow-init`, `airflow-webserver`, `airflow-scheduler`, executor `LocalExecutor`). Bestaande DAGs:

- [`combined_weather_pipeline`](dags/combined_weather_pipeline.py) — straling van 3 bronnen, parallel, daarna combineren en laden.
- [`csv_pipelines.py`](dags/csv_pipelines.py) — genereert dynamisch één DAG per CSV in `datasets/`.

### 8.1 Geplande uitbreiding — DAG `energy_master_pipeline`

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

## 9. Open punten / next steps

- [ ] Bronvalidatie van eenheden (MWh vs kWh) voor `consumptie.csv` kolommen Vlaanderen.
- [ ] Schema voor `fact_productie`, `fact_consumptie`, `fact_wind_obs`, `dim_tijd` toevoegen aan `init.sql`.
- [ ] DAG `energy_master_pipeline` schrijven die de aparte CSV-DAGs opvolgt.
- [ ] Grafana dashboard "Hernieuwbaar vs verbruik" met capaciteitsfactor per bron.
- [ ] Data-quality DAG die gaps en outliers per bron detecteert.
