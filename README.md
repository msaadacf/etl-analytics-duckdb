# End-to-End ETL Pipeline for Analytics (built using Python, SQL and DuckDB)

I built this ETL as part of a course assignment. It is a production style, zero-cost, efficient ETL pipeline that ingests **raw CSV **and** external weather API data**, normalizes it into **relational tables**, applies **SQL transformations** (Bronze/Silver/Gold), runs **data quality validation**, and finally outputs **analytics ready marts** for later reporting.


## What this project does
- **Ingestion**: Loads raw CSV files and enriches with a real external API feed.
- **Relational modeling**: Normalizes into dimensions/facts (star-schema style).
- **SQL-first transformations**: Uses DuckDB SQL to build curated datasets.
- **Data quality gates**: Row count checks, null checks, and schema validation.
- **Analytics outputs**: Produces clean marts in **DuckDB** and **Parquet** for later BI/ML consumption.
- **Single-command run**: One script executes the entire pipeline end-to-end.

---

## Data sources
1) **Raw CSV (committed)**
- `data/raw/stores.csv` - store metadata (location and coordinates)
- `data/raw/transactions.csv` - transaction line items (date, sku, qty, price)

2) **External API**
- **Open Meteo Archive API** (daily temperature and precipitation)  
Weather acts as a realistic proxy for integrating external operational feeds.

---

## Pipeline architecture (Bronze -> Silver -> Gold)

### Bronze (raw landing)
- `bronze.stores_raw`
- `bronze.transactions_raw`

### Silver (clean with normalized relational model)
- `silver.dim_store` - store dimension (store attributes)
- `silver.dim_date` - date dimension derived from transactions
- `silver.fact_transactions` - cleaned fact table with computed `revenue`
- `silver.store_weather_daily` - external API feed normalized by store/day

### Gold (analytics-ready marts)
- `gold.daily_sales_by_store`  
  Daily store KPIs joined with weather features (ready for dashboards / modeling)
- `gold.category_kpis`  
  Category level sales and pricing KPIs (ready for reporting)


## Data quality checks
The pipeline stops if any of these fail:
- **Row counts**: Bronze/Silver/Gold must not be empty
- **Null checks**: Critical keys in `silver.fact_transactions` must not be null
- **Schema validation**: `gold.daily_sales_by_store` must contain expected columns


## Repo structure

```bash
etl-analytics-duckdb/
├─ README.md
├─ requirements.txt
├─ .gitignore
├─ data/
│  └─ raw/
│     ├─ transactions.csv
│     └─ stores.csv
└─ src/
   └─ etl.py
```
## How to run (Windows)

### Recommended on Windows PowerShell: run using venv python directly
```bash
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe src/etl.py
```

## Outputs (These are generated locally, not committed)

- `warehouse.duckdb` - local analytical warehouse (tables under bronze/silver/gold)
- `outputs/daily_sales_by_store.parquet` - analytics mart
- `outputs/category_kpis.parquet` - analytics mart

Thank you!
