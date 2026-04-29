# Data documentatie — Wind, Zon, Energieproductie & Consumptie

> Project: `combineren`
> Auteur: Stijn Voeten (AP Hogeschool Antwerpen, Data Engineering 2025-2026)
> Laatst bijgewerkt: 2026-04-29

Dit document bevat **uitsluitend metadata** van de datasets: schema, kolommen, eenheden, periodes, relaties en aanvullende technische metadata. Voor projectcontext, ETL-aanpak, opslag, orkestratie en data-catalog-pipeline zie [README.md](README.md).

---

## 1. Datasets — schema en semantiek

Alle CSV's bevinden zich in `datasets/`. Encoding: **UTF-8**. Scheiding: **komma**. Decimaal: **punt**. Timestamps zijn uurgranulair, in UTC tenzij anders vermeld.

### 1.1 `consumptie.csv` — Elektriciteitsverbruik

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

### 1.2 `productie_comnbined.csv` — Hernieuwbare productie

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

### 1.3 `v_wind_alles_compleet.csv` — Windsnelheidsobservaties

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

## 2. Relaties tussen variabelen

Gerichte relaties tussen kolommen van verschillende datasets. Deze worden ook geladen in `metadata_relation` (zie [README.md](README.md#data-catalog)).

| Bron-dataset | Bron-kolom | Doel-dataset | Doel-kolom | Relatie | Toelichting |
|---|---|---|---|---|---|
| `v_wind_alles_compleet` | `wind_*` | `productie_comnbined` | `vlaanderen_wind_kwh` | drives | Hogere windsnelheid → hogere windproductie (kubisch, P ∝ v³, tot cut-off) |
| `combined_weather` | `*_radiation` | `productie_comnbined` | `vlaanderen_zon_kwh` | drives | Quasi-lineair tijdens daglicht; 's nachts beide nul |
| `consumptie` | `tijd` | `consumptie` | `Elia totaal` | seasonal | Sterke dagelijkse en jaarlijkse seizoenaliteit |
| `productie_comnbined` | `*_kwh` | `consumptie` | `Elia totaal` | balance | Verschil productie − consumptie = netto invoer/uitvoer |
| `v_wind_alles_compleet` | `wind_*` | `v_wind_alles_compleet` | `wind_*` | cross-source | Hoge correlatie tussen stations, met locatie-bias |

**Join-sleutel:** in alle datasets is dat het **uurstempel**. Voor heterogene tijdzones (`consumptie.csv` lokaal vs. `productie/wind` UTC) moet bij het joinen genormaliseerd worden naar UTC.

---

## 3. Aanvullende metadata

| Aspect | Waarde |
|---|---|
| Talen in data | Nederlands (kolomnamen), getallen in EN-formaat |
| Bestandsformaat | CSV |
| Compressie | geen |
| Licenties bronnen | Elia: open (CC-BY), KMI Open Data: vrij gebruik met bronvermelding, Open-Meteo: CC-BY-4.0, Kaggle: per dataset (community) |
| Privacy | geen persoonsgegevens; alle data is geaggregeerd op regio of station |
| Dataverversing | Elia/KMI: dagelijks; Open-Meteo archive: ~5 dagen vertraging |
| Versie van deze export | snapshot van april 2026 |

### Bronnen

| Bron | Gebruikt voor | Type |
|---|---|---|
| **Elia Open Data** (https://opendata.elia.be) | productie zon/wind (`elia_zon_kwh`, `elia_wind_kwh`), totaalverbruik | officieel, netbeheerder |
| **VEKA / Vlaanderen Energie- en Klimaatagentschap** | regionale productie Vlaanderen | overheidsstatistiek |
| **KMI (meteo.be) AWS** | meteo-observaties (straling, wind) | officiële meteo |
| **Open-Meteo Archive API** | reanalysedata wind/straling | open API, wereldwijd |
| **ECMWF (via Open-Meteo)** | windsnelheid model | reanalyse/forecast |
| **Kaggle dataset Belgium weather** | tweede-hands meteo, kruisvalidatie | community |
