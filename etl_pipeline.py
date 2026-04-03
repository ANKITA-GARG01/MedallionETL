"""
============================================================
  Medallion Architecture ETL — Dual Database Pipeline
  Structured   (.csv / .xlsx)  → SQL Server
  Semi-structured (.json / .txt) → PostgreSQL (JSONB)
  Gold KPI tables               → SQL Server (always)
============================================================
  Run: python etl_pipeline.py
"""

import sys
import json
import warnings
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSONB
import config

warnings.filterwarnings("ignore")

# ── Log folder must exist before logger opens the file
Path(config.LOG_FILE).parent.mkdir(parents=True, exist_ok=True)

logger.remove()
logger.add(sys.stdout,      format="{time:HH:mm:ss} | {level:<8} | {message}", colorize=False)
logger.add(config.LOG_FILE, format="{time} | {level} | {message}", rotation="5 MB")

INPUT_DIR = Path(config.INPUT_DIR)


# ════════════════════════════════════════════════════════════
# ENGINE FACTORY
# ════════════════════════════════════════════════════════════

def ss_engine():
    """SQL Server engine — structured data."""
    return create_engine(
        config.get_ss_connection_string(),
        connect_args={"fast_executemany": True},
    )


def ss_master_engine():
    return create_engine(
        config.get_ss_master_connection_string(),
        isolation_level="AUTOCOMMIT",
        connect_args={"fast_executemany": True},
    )


def pg_engine():
    """PostgreSQL engine — semi-structured data."""
    return create_engine(config.get_pg_connection_string())


def pg_master_engine():
    return create_engine(
        config.get_pg_master_connection_string(),
        isolation_level="AUTOCOMMIT",
    )


# ════════════════════════════════════════════════════════════
# SHARED UTILITIES
# ════════════════════════════════════════════════════════════

def write_table(df, schema, table, engine, dialect="ss"):
    """Write DataFrame to schema.table. Handles both SS and PG syntax."""
    kwargs = dict(
        name=table, schema=schema, con=engine,
        if_exists="replace", index=False, chunksize=500,
    )
    if dialect == "pg":
        kwargs["method"] = "multi"   # PostgreSQL multi-row INSERT
    df.to_sql(**kwargs)


def log_audit(engine, dialect, layer, table, rows_in, rows_out, bad=0, notes=""):
    logger.info(f"[{layer.upper()}] {table} | in={rows_in}  out={rows_out}  bad={bad}  {notes}")
    db_label = "SQL Server" if dialect == "ss" else "PostgreSQL"
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO audit.etl_log
                    (run_timestamp, layer, table_name, rows_in, rows_out, bad_rows, notes)
                VALUES (:ts, :layer, :tbl, :ri, :ro, :bad, :notes)
            """),
            {
                "ts":    datetime.now(),
                "layer": layer,
                "tbl":   f"{table} [{db_label}]",
                "ri":    int(rows_in),
                "ro":    int(rows_out),
                "bad":   int(bad),
                "notes": str(notes),
            },
        )


def clean_string_col(series):
    """Strip whitespace; replace all null-like strings with pd.NA."""
    return (
        series.astype(str)
        .str.strip()
        .replace({"": pd.NA, "None": pd.NA, "nan": pd.NA,
                  "NaN": pd.NA, "NULL": pd.NA, "<NA>": pd.NA})
    )


def read_file(path):
    """Read any supported flat file — all values as strings initially."""
    ext = path.suffix.lower()
    if ext == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    if ext == ".json":
        df = pd.read_json(path)
        return df.astype(object).where(df.notna(), other=None)
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(path, dtype=str, engine="openpyxl", keep_default_na=False)
    if ext == ".txt":
        return pd.read_csv(path, sep="|", dtype=str, keep_default_na=False)
    raise ValueError(f"Unsupported extension: {ext}")


def row_to_jsonb(df):
    """
    Serialize each row to a JSON string for PostgreSQL JSONB storage.
    Preserves the full semi-structured record exactly as ingested.
    """
    def _to_json(row):
        return json.dumps(
            {k: (None if (v is None or (isinstance(v, float) and np.isnan(v)))
                 else v)
             for k, v in row.items() if not str(k).startswith("_")},
            default=str,
        )
    return df.apply(_to_json, axis=1)


# ════════════════════════════════════════════════════════════
# STEP 0 — BOOTSTRAP: create both databases + all schemas
# ════════════════════════════════════════════════════════════

def bootstrap_sqlserver():
    logger.info("  [SQL Server] Creating database and schemas...")
    master = ss_master_engine()
    with master.connect() as conn:
        exists = conn.execute(
            text(f"SELECT 1 FROM sys.databases WHERE name='{config.SS_DATABASE}'")
        ).fetchone()
        if not exists:
            conn.execute(text(f"CREATE DATABASE [{config.SS_DATABASE}]"))
            logger.success(f"  [SQL Server] Created: {config.SS_DATABASE}")
        else:
            logger.info(f"  [SQL Server] Already exists: {config.SS_DATABASE}")
    master.dispose()

    eng = ss_engine()
    with eng.begin() as conn:
        for schema in ("bronze", "silver", "gold", "audit"):
            conn.execute(text(
                f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='{schema}') "
                f"EXEC('CREATE SCHEMA [{schema}]')"
            ))
        # Audit log table (SQL Server syntax: IDENTITY)
        conn.execute(text("""
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA='audit' AND TABLE_NAME='etl_log'
            )
            CREATE TABLE audit.etl_log (
                log_id        INT IDENTITY(1,1) PRIMARY KEY,
                run_timestamp DATETIME2     NOT NULL,
                layer         NVARCHAR(20)  NOT NULL,
                table_name    NVARCHAR(150),
                rows_in       INT,
                rows_out      INT,
                bad_rows      INT,
                notes         NVARCHAR(500)
            )
        """))
    logger.success("  [SQL Server] Schemas ready: bronze | silver | gold | audit")
    eng.dispose()


def bootstrap_postgres():
    logger.info("  [PostgreSQL] Creating database and schemas...")
    master = pg_master_engine()
    with master.connect() as conn:
        exists = conn.execute(
            text(f"SELECT 1 FROM pg_database WHERE datname='{config.PG_DATABASE}'")
        ).fetchone()
        if not exists:
            conn.execute(text(f"CREATE DATABASE {config.PG_DATABASE}"))
            logger.success(f"  [PostgreSQL] Created: {config.PG_DATABASE}")
        else:
            logger.info(f"  [PostgreSQL] Already exists: {config.PG_DATABASE}")
    master.dispose()

    eng = pg_engine()
    with eng.begin() as conn:
        # PostgreSQL: CREATE SCHEMA IF NOT EXISTS (much simpler than SQL Server)
        for schema in ("bronze", "silver"):
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        # PostgreSQL audit table (SERIAL instead of IDENTITY)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.pg_etl_log (
                log_id        SERIAL PRIMARY KEY,
                run_timestamp TIMESTAMP    NOT NULL,
                layer         VARCHAR(20)  NOT NULL,
                table_name    VARCHAR(150),
                rows_in       INTEGER,
                rows_out      INTEGER,
                bad_rows      INTEGER,
                notes         VARCHAR(500)
            )
        """))
    logger.success("  [PostgreSQL] Schemas ready: bronze | silver")
    eng.dispose()


def bootstrap():
    logger.info("══ BOOTSTRAP ════════════════════════════════════════════")
    bootstrap_sqlserver()
    bootstrap_postgres()
    logger.success("Both databases ready.")


# ════════════════════════════════════════════════════════════
# STEP 1 — BRONZE
#   .csv / .xlsx  → SQL Server   (typed columns, structured)
#   .json / .txt  → PostgreSQL   (raw_data JSONB + typed cols)
# ════════════════════════════════════════════════════════════

def bronze_to_sqlserver(path, eng, audit_eng):
    """Ingest CSV or Excel into SQL Server bronze schema."""
    df = read_file(path)
    rows_in = len(df)
    df["_source_file"]    = path.name
    df["_file_extension"] = path.suffix.lower().lstrip(".")
    df["_ingested_at"]    = datetime.now().isoformat(timespec="seconds")
    tbl = path.stem.lower().replace("-", "_").replace(" ", "_")
    write_table(df, "bronze", tbl, eng, dialect="ss")
    log_audit(audit_eng, "ss", "bronze", f"bronze.{tbl}", rows_in, len(df))
    logger.info(f"  → SQL Server  bronze.{tbl}  ({rows_in} rows)")


def bronze_to_postgres(path, pg_eng, audit_eng):
    """
    Ingest JSON or TXT into PostgreSQL bronze schema.
    Adds a raw_data JSONB column — preserves the full semi-structured
    record so it can be queried with PostgreSQL -> operators.
    """
    df = read_file(path)
    rows_in = len(df)

    df["raw_data"] = row_to_jsonb(df)

    df["_source_file"]    = path.name
    df["_file_extension"] = path.suffix.lower().lstrip(".")
    df["_ingested_at"]    = datetime.now().isoformat(timespec="seconds")

    tbl = path.stem.lower().replace("-", "_").replace(" ", "_")
    df.to_sql(
    name=tbl,
    schema="bronze",
    con=pg_eng,
    if_exists="replace",
    index=False,
    chunksize=500,
    method="multi",
    dtype={"raw_data": JSONB},   # ← tells PostgreSQL to use JSONB not TEXT
)

    # GIN index on JSONB for fast -> operator queries
    with pg_eng.begin() as conn:
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_bronze_{tbl}_raw "
            f"ON bronze.{tbl} USING GIN (raw_data jsonb_path_ops)"
        ))

    log_audit(audit_eng, "ss", "bronze", f"bronze.{tbl}", rows_in, len(df),
              notes="PostgreSQL JSONB")
    logger.info(f"  → PostgreSQL  bronze.{tbl}  ({rows_in} rows) [JSONB]")


def load_bronze(ss_eng, pg_eng, audit_eng):
    logger.info("══ BRONZE ═══════════════════════════════════════════════")
    files = [
        f for f in INPUT_DIR.glob("*.*")
        if f.suffix.lower() in config.SS_EXTENSIONS | config.PG_EXTENSIONS
    ]
    if not files:
        logger.warning(f"No supported files found in {INPUT_DIR}")
        return

    for path in files:
        logger.info(f"  Reading: {path.name}")
        try:
            ext = path.suffix.lower()
            if ext in config.SS_EXTENSIONS:
                bronze_to_sqlserver(path, ss_eng, audit_eng)
            else:
                bronze_to_postgres(path, pg_eng, audit_eng)
        except Exception as e:
            logger.error(f"  Bronze failed for {path.name}: {e}")

    logger.success("Bronze complete.")


# ════════════════════════════════════════════════════════════
# STEP 2 — SILVER
#   orders / products  → cleaned → SQL Server silver
#   customers / returns → cleaned → PostgreSQL silver
# ════════════════════════════════════════════════════════════

def silver_orders(ss_eng):
    df = pd.read_sql("SELECT * FROM bronze.orders", ss_eng)
    ri = len(df)
    df = df[[c for c in df.columns if not c.startswith("_")]]

    df["status"]       = clean_string_col(df["status"]).str.lower()
    df["order_date"]   = pd.to_datetime(df["order_date"], errors="coerce", dayfirst=True)
    df["quantity"]     = pd.to_numeric(df["quantity"],     errors="coerce").fillna(1).astype(int)
    df["unit_price"]   = pd.to_numeric(df["unit_price"],   errors="coerce")
    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce").fillna(0)
    df["customer_id"]  = clean_string_col(df["customer_id"]).fillna("UNKNOWN")

    before = len(df)
    df = df.dropna(subset=["order_id", "product_id", "unit_price"])
    df["net_revenue"] = (
        df["unit_price"] * df["quantity"] * (1 - df["discount_pct"] / 100)
    ).round(2)
    df["_silver_at"] = datetime.now().isoformat(timespec="seconds")
    return df, ri, len(df), before - len(df)


def silver_products(ss_eng):
    df = pd.read_sql("SELECT * FROM bronze.products", ss_eng)
    ri = len(df)
    df = df[[c for c in df.columns if not c.startswith("_")]]

    df["cost_price"]   = pd.to_numeric(df["cost_price"],  errors="coerce")
    df["sell_price"]   = pd.to_numeric(df["sell_price"],  errors="coerce")
    df["stock_qty"]    = pd.to_numeric(df["stock_qty"],   errors="coerce").fillna(0).astype(int)
    df["product_name"] = clean_string_col(df["product_name"])
    df["category"]     = clean_string_col(df["category"])
    df["is_active"]    = (
        clean_string_col(df["is_active"]).str.upper().map({"YES": True, "NO": False})
    )
    df["margin_pct"] = np.where(
        df["sell_price"].notna() & (df["sell_price"] != 0),
        ((df["sell_price"] - df["cost_price"]) / df["sell_price"] * 100).round(2),
        np.nan,
    )
    before = len(df)
    df = df.dropna(subset=["product_id", "product_name"])
    df["_silver_at"] = datetime.now().isoformat(timespec="seconds")
    return df, ri, len(df), before - len(df)


def silver_customers(pg_eng):
    df = pd.read_sql("SELECT * FROM bronze.customers", pg_eng)
    ri = len(df)
    df = df[[c for c in df.columns if not c.startswith("_") and c != "raw_data"]]

    df["full_name"]   = clean_string_col(df["full_name"]).str.title()
    df["email"]       = clean_string_col(df["email"]).str.lower()
    df["city"]        = clean_string_col(df["city"]).str.title()
    df["tier"]        = clean_string_col(df["tier"]).str.title()
    df["signup_date"] = pd.to_datetime(
        clean_string_col(df["signup_date"]), errors="coerce"
    )
    before = len(df)
    df = df.dropna(subset=["customer_id"])
    df["_silver_at"] = datetime.now().isoformat(timespec="seconds")
    return df, ri, len(df), before - len(df)


def silver_returns(pg_eng):
    df = pd.read_sql("SELECT * FROM bronze.returns", pg_eng)
    ri = len(df)
    df = df[[c for c in df.columns if not c.startswith("_") and c != "raw_data"]]

    df["return_date"]   = pd.to_datetime(
        clean_string_col(df["return_date"]), errors="coerce"
    )
    df["refund_amount"] = pd.to_numeric(df["refund_amount"], errors="coerce").fillna(0)
    df["status"]        = clean_string_col(df["status"]).str.lower()
    df["reason"]        = clean_string_col(df["reason"])

    before = len(df)
    df = df.dropna(subset=["return_id", "return_date", "order_id"])
    df["_silver_at"] = datetime.now().isoformat(timespec="seconds")
    return df, ri, len(df), before - len(df)


def load_silver(ss_eng, pg_eng, audit_eng):
    logger.info("══ SILVER ═══════════════════════════════════════════════")

    # SQL Server silver tables
    for tbl, fn in [("orders", silver_orders), ("products", silver_products)]:
        logger.info(f"  Cleaning: {tbl} → SQL Server")
        try:
            df, ri, ro, bad = fn(ss_eng)
            write_table(df, "silver", tbl, ss_eng, dialect="ss")
            log_audit(audit_eng, "ss", "silver", f"silver.{tbl}", ri, ro, bad)
        except Exception as e:
            logger.error(f"  silver.{tbl} (SS) failed: {e}")

    # PostgreSQL silver tables
    for tbl, fn in [("customers", silver_customers), ("returns", silver_returns)]:
        logger.info(f"  Cleaning: {tbl} → PostgreSQL")
        try:
            df, ri, ro, bad = fn(pg_eng)
            write_table(df, "silver", tbl, pg_eng, dialect="pg")
            log_audit(audit_eng, "pg", "silver", f"silver.{tbl}", ri, ro, bad,
                      notes="PostgreSQL")
        except Exception as e:
            logger.error(f"  silver.{tbl} (PG) failed: {e}")

    logger.success("Silver complete.")


# ════════════════════════════════════════════════════════════
# STEP 3 — GOLD  (all tables written to SQL Server)
#
#   Pure SQL Server queries:
#     daily_revenue_summary   — silver.orders (SS)
#     product_performance     — silver.orders + silver.products (SS)
#
#   Cross-DB (pandas merge):
#     customer_lifetime_value — silver.customers (PG) + silver.orders (SS)
#     city_revenue_summary    — silver.customers (PG) + silver.orders (SS)
#     return_analysis         — silver.returns (PG) → written to SS gold
# ════════════════════════════════════════════════════════════

def gold_daily_revenue(ss_eng, audit_eng):
    """Pure SQL Server — silver.orders only."""
    sql = """
        SELECT
            CAST(order_date AS DATE)                              AS order_date,
            COUNT(DISTINCT order_id)                              AS total_orders,
            ROUND(SUM(net_revenue), 2)                            AS gross_revenue,
            ROUND(SUM(CASE WHEN status='completed'
                     THEN net_revenue ELSE 0 END), 2)             AS completed_revenue,
            COUNT(CASE WHEN status='cancelled' THEN 1 END)        AS cancelled_orders,
            GETDATE()                                             AS gold_created_at
        FROM silver.orders
        WHERE order_date IS NOT NULL
        GROUP BY CAST(order_date AS DATE)
        ORDER BY CAST(order_date AS DATE)
    """
    df = pd.read_sql(sql, ss_eng)
    write_table(df, "gold", "daily_revenue_summary", ss_eng, dialect="ss")
    log_audit(audit_eng, "ss", "gold", "gold.daily_revenue_summary",
              len(df), len(df), notes="Source: SQL Server silver.orders")


def gold_product_performance(ss_eng, audit_eng):
    """Pure SQL Server — silver.orders + silver.products."""
    sql = """
        SELECT
            p.product_id,
            p.product_name,
            p.category,
            p.margin_pct,
            p.stock_qty,
            COUNT(DISTINCT o.order_id)                            AS times_ordered,
            ISNULL(SUM(o.quantity), 0)                            AS units_sold,
            ROUND(ISNULL(SUM(o.net_revenue), 0), 2)              AS total_revenue,
            GETDATE()                                             AS gold_created_at
        FROM silver.products p
        LEFT JOIN silver.orders o ON p.product_id = o.product_id
        GROUP BY p.product_id, p.product_name, p.category,
                 p.margin_pct, p.stock_qty
    """
    df = pd.read_sql(sql, ss_eng)
    write_table(df, "gold", "product_performance", ss_eng, dialect="ss")
    log_audit(audit_eng, "ss", "gold", "gold.product_performance",
              len(df), len(df), notes="Source: SQL Server silver")


def gold_customer_lifetime_value(ss_eng, pg_eng, audit_eng):
    """
    Cross-DB merge:
      silver.customers  ← PostgreSQL
      silver.orders     ← SQL Server
    Merge in pandas → write to SQL Server gold.
    """
    customers = pd.read_sql(
        "SELECT customer_id, full_name, city, tier FROM silver.customers", pg_eng
    )
    orders = pd.read_sql(
        "SELECT customer_id, order_id, net_revenue, order_date FROM silver.orders", ss_eng
    )
    returns_count = pd.read_sql(
        "SELECT order_id, COUNT(return_id) AS return_count FROM silver.returns GROUP BY order_id",
        pg_eng
    )

    # Merge orders with returns
    orders = orders.merge(returns_count, on="order_id", how="left")
    orders["return_count"] = orders["return_count"].fillna(0).astype(int)

    # Aggregate orders per customer
    agg = orders.groupby("customer_id").agg(
        total_orders   = ("order_id",    "count"),
        lifetime_value = ("net_revenue", "sum"),
        avg_order_value= ("net_revenue", "mean"),
        last_order_date= ("order_date",  "max"),
        total_returns  = ("return_count","sum"),
    ).reset_index()
    agg["lifetime_value"]  = agg["lifetime_value"].round(2)
    agg["avg_order_value"] = agg["avg_order_value"].round(2)

    # Join with customer master from PostgreSQL
    df = customers.merge(agg, on="customer_id", how="left")
    df["total_orders"]   = df["total_orders"].fillna(0).astype(int)
    df["lifetime_value"] = df["lifetime_value"].fillna(0).round(2)
    df["avg_order_value"]= df["avg_order_value"].fillna(0).round(2)
    df["total_returns"]  = df["total_returns"].fillna(0).astype(int)
    df["gold_created_at"]= datetime.now().isoformat(timespec="seconds")

    write_table(df, "gold", "customer_lifetime_value", ss_eng, dialect="ss")
    log_audit(audit_eng, "ss", "gold", "gold.customer_lifetime_value",
              len(df), len(df),
              notes="Cross-DB: customers(PostgreSQL) + orders(SQL Server)")


def gold_city_revenue(ss_eng, pg_eng, audit_eng):
    """
    Cross-DB merge:
      silver.customers  ← PostgreSQL
      silver.orders     ← SQL Server
    """
    customers = pd.read_sql(
        "SELECT customer_id, city FROM silver.customers", pg_eng
    )
    orders = pd.read_sql(
        "SELECT customer_id, order_id, net_revenue FROM silver.orders", ss_eng
    )

    merged = customers.merge(orders, on="customer_id", how="left")
    df = merged.groupby("city").agg(
        total_customers = ("customer_id", "nunique"),
        total_orders    = ("order_id",    "nunique"),
        total_revenue   = ("net_revenue", "sum"),
        avg_order_value = ("net_revenue", "mean"),
    ).reset_index()
    df["total_revenue"]   = df["total_revenue"].round(2)
    df["avg_order_value"] = df["avg_order_value"].round(2)
    df["gold_created_at"] = datetime.now().isoformat(timespec="seconds")

    write_table(df, "gold", "city_revenue_summary", ss_eng, dialect="ss")
    log_audit(audit_eng, "ss", "gold", "gold.city_revenue_summary",
              len(df), len(df),
              notes="Cross-DB: customers(PostgreSQL) + orders(SQL Server)")


def gold_return_analysis(ss_eng, pg_eng, audit_eng):
    """
    Read silver.returns from PostgreSQL,
    aggregate in pandas, write to SQL Server gold.
    """
    df = pd.read_sql("SELECT reason, status, refund_amount FROM silver.returns", pg_eng)
    agg = df.groupby(["reason", "status"]).agg(
        return_count   = ("refund_amount", "count"),
        total_refunded = ("refund_amount", "sum"),
        avg_refund     = ("refund_amount", "mean"),
    ).reset_index()
    agg.columns     = ["reason", "return_status", "return_count",
                        "total_refunded", "avg_refund"]
    agg["total_refunded"]  = agg["total_refunded"].round(2)
    agg["avg_refund"]      = agg["avg_refund"].round(2)
    agg["gold_created_at"] = datetime.now().isoformat(timespec="seconds")

    write_table(agg, "gold", "return_analysis", ss_eng, dialect="ss")
    log_audit(audit_eng, "ss", "gold", "gold.return_analysis",
              len(agg), len(agg),
              notes="Source: PostgreSQL silver.returns → written to SQL Server")


def load_gold(ss_eng, pg_eng, audit_eng):
    logger.info("══ GOLD ═════════════════════════════════════════════════")
    logger.info("  All gold tables written to SQL Server")

    tasks = [
        ("daily_revenue_summary",    lambda: gold_daily_revenue(ss_eng, audit_eng)),
        ("product_performance",      lambda: gold_product_performance(ss_eng, audit_eng)),
        ("customer_lifetime_value",  lambda: gold_customer_lifetime_value(ss_eng, pg_eng, audit_eng)),
        ("city_revenue_summary",     lambda: gold_city_revenue(ss_eng, pg_eng, audit_eng)),
        ("return_analysis",          lambda: gold_return_analysis(ss_eng, pg_eng, audit_eng)),
    ]

    for name, fn in tasks:
        logger.info(f"  Building: gold.{name}")
        try:
            fn()
        except Exception as e:
            logger.error(f"  gold.{name} failed: {e}")

    logger.success("Gold complete.")


# ════════════════════════════════════════════════════════════
# STEP 4 — SUMMARY
# ════════════════════════════════════════════════════════════

def print_summary(ss_eng, pg_eng):
    logger.info("══ SUMMARY ══════════════════════════════════════════════")
    try:
        audit = pd.read_sql(
            "SELECT layer, table_name, rows_in, rows_out, bad_rows "
            "FROM audit.etl_log ORDER BY log_id",
            ss_eng,
        )
        print("\n" + audit.to_string(index=False))

        print("\n── SQL Server gold previews ─────────────────────────────")
        for tbl in ("daily_revenue_summary", "customer_lifetime_value",
                    "product_performance", "city_revenue_summary"):
            try:
                df = pd.read_sql(f"SELECT TOP 3 * FROM gold.{tbl}", ss_eng)
                print(f"\ngold.{tbl}:\n{df.to_string(index=False)}")
            except Exception as e:
                logger.warning(f"  Preview failed for gold.{tbl}: {e}")

        print("\n── PostgreSQL JSONB query examples ─────────────────────")
        print("  -- Extract email from raw bronze JSON:")
        print("  SELECT raw_data->>'email' FROM bronze.customers;")
        print()
        print("  -- Find completed orders in raw bronze:")
        print("  SELECT raw_data->>'order_id', raw_data->>'unit_price'")
        print("  FROM bronze.orders")
        print("  WHERE raw_data->>'status' = 'completed';")

    except Exception as e:
        logger.error(f"Summary failed: {e}")


# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════

def main():
    start = datetime.now()
    logger.info("=" * 56)
    logger.info("  Dual-DB Medallion ETL Pipeline — Starting")
    logger.info(f"  SQL Server : {config.SS_SERVER} / {config.SS_DATABASE}")
    logger.info(f"  PostgreSQL : {config.PG_HOST}:{config.PG_PORT} / {config.PG_DATABASE}")
    logger.info(f"  Input      : {config.INPUT_DIR}")
    logger.info("=" * 56)

    ss_eng = pg_eng = audit_eng = None
    try:
        bootstrap()
        ss_eng    = ss_engine()
        pg_eng    = pg_engine()
        audit_eng = ss_eng          # audit log lives in SQL Server

        load_bronze(ss_eng, pg_eng, audit_eng)
        load_silver(ss_eng, pg_eng, audit_eng)
        load_gold(ss_eng, pg_eng, audit_eng)
        print_summary(ss_eng, pg_eng)

        elapsed = (datetime.now() - start).total_seconds()
        logger.success(f"Pipeline complete in {elapsed:.1f}s")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        for eng in (ss_eng, pg_eng):
            try:
                if eng:
                    eng.dispose()
            except Exception:
                pass


if __name__ == "__main__":
    main()