# 🦠 COVID Data Warehouse
### End-to-End Analytics Pipeline — dbt + Snowflake + Apache Airflow

![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│              COVID DATA WAREHOUSE                                    │
│         dbt + Snowflake + Airflow Medallion Architecture            │
└─────────────────────────────────────────────────────────────────────┘

  [Our World in Data — COVID CSV]
  402,910 rows across 243 countries
        │
        ▼  Python + Airflow DAG (daily schedule)
  ┌─────────────────────────────────────┐
  │           RAW LAYER                 │
  │      RAW_COVID_DATA                 │
  │  • Direct load from OWID CSV        │
  │  • No transforms applied            │
  │  • 402,910 rows                     │
  │  • Snowflake — COVID_DW.RAW         │
  └──────────────┬──────────────────────┘
                 │
                 ▼  dbt staging model (view)
  ┌─────────────────────────────────────┐
  │           STAGING LAYER             │
  │      stg_covid (view)               │
  │  • Cleaned & typed columns          │
  │  • NULLIF zeros replaced            │
  │  • case_fatality_rate calculated    │
  │  • vaccination_rate_pct calculated  │
  │  • Continent filter applied         │
  └──────────────┬──────────────────────┘
                 │
                 ▼  dbt mart models (tables)
  ┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
  │  mart_country_kpis   │  │  mart_monthly_trends  │  │  mart_vaccination    │
  │                      │  │                       │  │  _leaders            │
  │  • 243 countries     │  │  • 333 months         │  │  • Top 50 countries  │
  │  • Total cases/      │  │  • Per continent      │  │  • By vaccination    │
  │    deaths/vaxx       │  │  • Monthly new cases  │  │    rate              │
  │  • Global rankings   │  │  • CFR trends         │  │  • GDP correlation   │
  │  • GDP correlation   │  │  • Vaxx adoption      │  │  • CFR comparison    │
  └──────────────────────┘  └──────────────────────┘  └──────────────────────┘
                 │
                 ▼  Airflow orchestration
  ┌─────────────────────────────────────┐
  │        AIRFLOW DAG (daily)          │
  │  load_raw → dbt_staging →           │
  │  dbt_marts → dbt_test               │
  │  • Retry logic (2 retries)          │
  │  • 8 automated dbt tests            │
  └─────────────────────────────────────┘
```

---

## 📊 Results

| Metric | Value |
|---|---|
| Total Rows Processed | 402,910 |
| Countries Covered | 243 |
| Months of Data | 333 |
| dbt Models Built | 4 |
| dbt Tests Passing | 8 / 8 |
| Airflow Tasks | 4 |
| Pipeline Schedule | Daily |
| Top Vaccinated Region | Europe |

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| **Snowflake** | Cloud data warehouse — RAW, STAGING, MARTS schemas |
| **dbt Core** | Data transformation, testing, documentation |
| **Apache Airflow** | Pipeline orchestration — daily DAG |
| **Astronomer Astro CLI** | Local Airflow via Docker |
| **Python 3.11** | Data ingestion script |
| **pandas** | DataFrame transformations |
| **Our World in Data** | Public COVID-19 dataset source |

---

## 📁 Project Structure

```
covid-data-warehouse/
│
├── covid_dw/                        # dbt project
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_covid.sql        # Cleaned + typed staging view
│   │   │   ├── stg_covid.yml        # Column tests + descriptions
│   │   │   └── sources.yml          # Raw source definition
│   │   └── marts/
│   │       ├── mart_country_kpis.sql      # Country-level KPIs + rankings
│   │       ├── mart_monthly_trends.sql    # Monthly trends by continent
│   │       ├── mart_vaccination_leaders.sql # Top 50 vaccination countries
│   │       └── marts.yml                  # Mart tests + descriptions
│   └── dbt_project.yml
│
├── airflow/
│   └── dags/
│       └── covid_pipeline.py        # Airflow DAG — full pipeline
│
├── load_raw.py                      # Standalone raw loader script
└── README.md
```

---

## 🚀 How to Run

### Prerequisites
- Snowflake account ([free trial](https://trial.snowflake.com))
- Python 3.11
- dbt-snowflake: `pip install dbt-snowflake`
- Docker Desktop
- Astronomer Astro CLI: `winget install -e --id Astronomer.Astro`

### Steps

**1. Clone the repo**
```bash
git clone https://github.com/abhinav2627/covid-data-warehouse.git
cd covid-data-warehouse
```

**2. Set up Snowflake**
Run this in a Snowflake SQL worksheet:
```sql
CREATE WAREHOUSE IF NOT EXISTS COVID_WH WITH WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 60;
CREATE DATABASE IF NOT EXISTS COVID_DW;
CREATE SCHEMA IF NOT EXISTS COVID_DW.RAW;
CREATE SCHEMA IF NOT EXISTS COVID_DW.STAGING;
CREATE SCHEMA IF NOT EXISTS COVID_DW.MARTS;
```

**3. Configure dbt profile**
Create `~/.dbt/profiles.yml`:
```yaml
covid_dw:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: YOUR_ACCOUNT
      user: YOUR_USER
      password: YOUR_PASSWORD
      role: ACCOUNTADMIN
      warehouse: COVID_WH
      database: COVID_DW
      schema: RAW
      threads: 4
```

**4. Load raw data**
```bash
python load_raw.py
```

**5. Run dbt**
```bash
cd covid_dw
dbt run
dbt test
dbt docs generate && dbt docs serve
```

**6. Start Airflow**
```bash
cd airflow
astro dev start
```
Open http://localhost:8080 → trigger `covid_data_pipeline` DAG.

---

## 🔍 dbt Models

### `stg_covid` (View — STAGING schema)
Cleans the raw data:
- Replaces zero values with NULL using `NULLIF`
- Calculates `case_fatality_rate` = deaths / cases × 100
- Calculates `vaccination_rate_pct` = fully vaccinated / population × 100
- Filters out continental aggregates (World, Asia, etc.)

### `mart_country_kpis` (Table — MARTS schema)
One row per country with:
- Latest total cases, deaths, vaccinations
- Global rank by cases, deaths, and vaccination rate
- GDP per capita for economic correlation analysis

### `mart_monthly_trends` (Table — MARTS schema)
One row per continent per month:
- Monthly new cases and deaths
- Average vaccination rate adoption over time
- Average case fatality rate trend

### `mart_vaccination_leaders` (Table — MARTS schema)
Top 50 countries by vaccination rate:
- Vaccination rate vs GDP correlation
- Case fatality rate comparison
- Population context

---

## ✅ dbt Tests

| Test | Model | Status |
|---|---|---|
| not_null — iso_code | stg_covid | ✅ |
| not_null — country | stg_covid | ✅ |
| not_null — report_date | stg_covid | ✅ |
| not_null — country | mart_country_kpis | ✅ |
| unique — country | mart_country_kpis | ✅ |
| not_null — rank_by_cases | mart_country_kpis | ✅ |
| not_null — month | mart_monthly_trends | ✅ |
| not_null — vaccination_rank | mart_vaccination_leaders | ✅ |

---

## 🗺️ Portfolio Roadmap

This is **Project 3** of a 6-month data engineering portfolio series:

- ✅ Project 1 — [Financial Transactions Lakehouse](https://github.com/abhinav2627/financial-transactions-lakehouse)
- ✅ Project 2 — [NYC Taxi Real-Time Pipeline](https://github.com/abhinav2627/nyc-taxi-realtime-pipeline)
- ✅ Project 3 — COVID Data Warehouse (dbt + Snowflake + Airflow)
- 🔜 Project 4 — E-Commerce ETL Pipeline
- 🔜 Project 5 — Weather API Data Lake

---

## 👤 Author

**Abhinav Mandal**
- LinkedIn: [linkedin.com/in/abhinavmandal27](https://linkedin.com/in/abhinavmandal27)
- GitHub: [github.com/abhinav2627](https://github.com/abhinav2627)

---

## 📄 License

MIT License — feel free to use this as a template for your own portfolio.
