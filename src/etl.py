import duckdb
import pandas as pd
import requests
from datetime import date
from pathlib import Path


def get_paths():
    repo_root = Path(__file__).resolve().parents[1]
    raw_dir = repo_root / "data" / "raw"
    db_path = repo_root / "warehouse.duckdb"
    outputs_dir = repo_root / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)

    return {
        "repo_root": repo_root,
        "raw_dir": raw_dir,
        "db_path": db_path,
        "outputs_dir": outputs_dir,
    }


def fetch_weather_daily(lat, lon, start, end):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": "temperature_2m_mean,precipitation_sum",
        "timezone": "UTC",
    }

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    payload = r.json()

    daily = payload.get("daily", {})
    df = pd.DataFrame(daily)

    if df.empty:
        return df

    df.rename(columns={"time": "date"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["temperature_2m_mean"] = pd.to_numeric(df["temperature_2m_mean"], errors="coerce")
    df["precipitation_sum"] = pd.to_numeric(df["precipitation_sum"], errors="coerce")

    return df[["date", "temperature_2m_mean", "precipitation_sum"]]


def connect(db_path):
    con = duckdb.connect(str(db_path))
    con.execute("PRAGMA threads=4;")
    return con


def load_bronze(con, raw_dir):
    stores_path = raw_dir / "stores.csv"
    txns_path = raw_dir / "transactions.csv"

    if not stores_path.exists():
        raise FileNotFoundError("Missing " + str(stores_path))
    if not txns_path.exists():
        raise FileNotFoundError("Missing " + str(txns_path))

    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

    con.execute("DROP TABLE IF EXISTS bronze.stores_raw;")
    con.execute("DROP TABLE IF EXISTS bronze.transactions_raw;")

    con.execute(
        """
        CREATE TABLE bronze.stores_raw AS
        SELECT * FROM read_csv_auto(?, HEADER=TRUE);
        """,
        [stores_path.as_posix()],
    )

    con.execute(
        """
        CREATE TABLE bronze.transactions_raw AS
        SELECT * FROM read_csv_auto(?, HEADER=TRUE);
        """,
        [txns_path.as_posix()],
    )


def build_silver(con):
    con.execute("DROP TABLE IF EXISTS silver.dim_store;")
    con.execute("DROP TABLE IF EXISTS silver.dim_date;")
    con.execute("DROP TABLE IF EXISTS silver.fact_transactions;")

    con.execute(
        """
        CREATE TABLE silver.dim_store AS
        SELECT
            CAST(store_id AS INTEGER) AS store_id,
            CAST(store_name AS VARCHAR) AS store_name,
            CAST(city AS VARCHAR) AS city,
            CAST(state AS VARCHAR) AS state,
            CAST(latitude AS DOUBLE) AS latitude,
            CAST(longitude AS DOUBLE) AS longitude
        FROM bronze.stores_raw;
        """
    )

    con.execute(
        """
        CREATE TABLE silver.fact_transactions AS
	SELECT
    		CAST(t.transaction_id AS BIGINT) AS transaction_id,
    		CAST(t.store_id AS INTEGER) AS store_id,
    		CAST(t.txn_date AS DATE) AS txn_date,
    		CAST(t.sku AS VARCHAR) AS sku,
    		CAST(t.category AS VARCHAR) AS category,
    		CAST(t.qty AS INTEGER) AS qty,
    		CAST(t.unit_price AS DOUBLE) AS unit_price,
    		CAST(t.qty * t.unit_price AS DOUBLE) AS revenue
	FROM bronze.transactions_raw t
	WHERE
    		t.transaction_id IS NOT NULL
    		AND t.store_id IS NOT NULL
    		AND t.txn_date IS NOT NULL
    		AND t.qty IS NOT NULL AND t.qty > 0
    		AND t.unit_price IS NOT NULL AND t.unit_price > 0;
        """
    )

    con.execute(
        """
        CREATE TABLE silver.dim_date AS
        SELECT DISTINCT
            txn_date AS date,
            EXTRACT(year FROM txn_date)::INTEGER AS year,
            EXTRACT(month FROM txn_date)::INTEGER AS month,
            EXTRACT(day FROM txn_date)::INTEGER AS day,
            EXTRACT(dow FROM txn_date)::INTEGER AS day_of_week
        FROM silver.fact_transactions;
        """
    )


def ingest_weather_to_silver(con):
    con.execute("DROP TABLE IF EXISTS silver.store_weather_daily;")

    start_d, end_d = con.execute(
        "SELECT MIN(txn_date), MAX(txn_date) FROM silver.fact_transactions;"
    ).fetchone()

    if start_d is None or end_d is None:
        raise RuntimeError("No transactions in silver.fact_transactions")

    stores = con.execute(
        "SELECT store_id, latitude, longitude FROM silver.dim_store;"
    ).fetchall()

    frames = []
    for store_id, lat, lon in stores:
        df = fetch_weather_daily(float(lat), float(lon), start_d, end_d)
        if df.empty:
            continue
        df.insert(0, "store_id", int(store_id))
        frames.append(df)

    if not frames:
        con.execute(
            """
            CREATE TABLE silver.store_weather_daily (
              store_id INTEGER,
              date DATE,
              temp_mean_c DOUBLE,
              precip_mm DOUBLE
            );
            """
        )
        return

    weather_df = pd.concat(frames, ignore_index=True)
    con.register("weather_df", weather_df)

    con.execute(
        """
        CREATE TABLE silver.store_weather_daily AS
        SELECT
          CAST(store_id AS INTEGER) AS store_id,
          CAST(date AS DATE) AS date,
          CAST(temperature_2m_mean AS DOUBLE) AS temp_mean_c,
          CAST(precipitation_sum AS DOUBLE) AS precip_mm
        FROM weather_df;
        """
    )

    con.unregister("weather_df")


def build_gold(con):
    con.execute("DROP TABLE IF EXISTS gold.daily_sales_by_store;")
    con.execute("DROP TABLE IF EXISTS gold.category_kpis;")

    con.execute(
        """
        CREATE TABLE gold.daily_sales_by_store AS
        WITH daily AS (
          SELECT
            store_id,
            txn_date AS date,
            COUNT(*) AS txn_count,
            SUM(qty) AS units_sold,
            SUM(revenue) AS revenue
          FROM silver.fact_transactions
          GROUP BY 1,2
        )
        SELECT
          d.store_id,
          s.store_name,
          s.city,
          s.state,
          d.date,
          d.txn_count,
          d.units_sold,
          d.revenue,
          w.temp_mean_c,
          w.precip_mm
        FROM daily d
        JOIN silver.dim_store s
          ON s.store_id = d.store_id
        LEFT JOIN silver.store_weather_daily w
          ON w.store_id = d.store_id AND w.date = d.date
        ORDER BY d.date, d.store_id;
        """
    )

    con.execute(
        """
        CREATE TABLE gold.category_kpis AS
        SELECT
          category,
          COUNT(*) AS txn_lines,
          SUM(qty) AS units_sold,
          SUM(revenue) AS revenue,
          AVG(unit_price) AS avg_unit_price
        FROM silver.fact_transactions
        GROUP BY 1
        ORDER BY revenue DESC;
        """
    )


def schema_of(con, table):
    info = con.execute("PRAGMA table_info(?);", [table]).fetchall()
    # columns: cid, name, type, notnull, dflt_value, pk
    return [(row[1], row[2]) for row in info]


def quality_checks(con):
    bronze_txn = con.execute("SELECT COUNT(*) FROM bronze.transactions_raw;").fetchone()[0]
    silver_txn = con.execute("SELECT COUNT(*) FROM silver.fact_transactions;").fetchone()[0]
    gold_daily = con.execute("SELECT COUNT(*) FROM gold.daily_sales_by_store;").fetchone()[0]

    if bronze_txn <= 0:
        raise RuntimeError("DQ fail: bronze.transactions_raw empty")
    if silver_txn <= 0:
        raise RuntimeError("DQ fail: silver.fact_transactions empty")
    if gold_daily <= 0:
        raise RuntimeError("DQ fail: gold.daily_sales_by_store empty")

    nulls = con.execute(
        """
        SELECT
          SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) AS null_transaction_id,
          SUM(CASE WHEN store_id IS NULL THEN 1 ELSE 0 END) AS null_store_id,
          SUM(CASE WHEN txn_date IS NULL THEN 1 ELSE 0 END) AS null_txn_date
        FROM silver.fact_transactions;
        """
    ).fetchone()

    if any(int(x) > 0 for x in nulls):
        raise RuntimeError("DQ fail: nulls in silver.fact_transactions: " + str(nulls))

    expected = {
        "store_id", "store_name", "city", "state", "date",
        "txn_count", "units_sold", "revenue", "temp_mean_c", "precip_mm"
    }
    actual = set([c for c, _ in schema_of(con, "gold.daily_sales_by_store")])
    missing = expected - actual
    if missing:
        raise RuntimeError("DQ fail: missing columns in gold.daily_sales_by_store: " + str(sorted(missing)))

    return {
        "bronze.transactions_raw": int(bronze_txn),
        "silver.fact_transactions": int(silver_txn),
        "gold.daily_sales_by_store": int(gold_daily),
    }


def export_outputs(con, outputs_dir):
    out1 = (outputs_dir / "daily_sales_by_store.parquet").as_posix()
    out2 = (outputs_dir / "category_kpis.parquet").as_posix()

    con.execute(
        "COPY gold.daily_sales_by_store TO ? (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE);",
        [out1],
    )
    con.execute(
        "COPY gold.category_kpis TO ? (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE);",
        [out2],
    )


def main():
    paths = get_paths()

    con = connect(paths["db_path"])
    try:
        load_bronze(con, paths["raw_dir"])
        build_silver(con)
        ingest_weather_to_silver(con)
        build_gold(con)
        metrics = quality_checks(con)
        export_outputs(con, paths["outputs_dir"])

        print("ETL success")
        for k, v in metrics.items():
            print(k + ": " + str(v) + " rows")
        print("Warehouse: " + str(paths["db_path"]))
        print("Outputs: " + str(paths["outputs_dir"]))
    finally:
        con.close()


if __name__ == "__main__":
    main()