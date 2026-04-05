import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()

USE_WINDOWS_AUTH = True          # True = Windows Auth | False = SQL login

SS_SERVER   = os.getenv("SS_SERVER")
SS_DATABASE = os.getenv("SS_DATABASE")
SS_ODBC     = os.getenv("SS_ODBC")

PG_HOST     = os.getenv("PG_HOST")
PG_PORT     = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USERNAME = os.getenv("PG_USERNAME")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# ── Shared folder paths
INPUT_DIR = r"C:\MedallionETL\input"
LOG_FILE  = r"C:\MedallionETL\logs\etl_audit.log"

# ── File extension routing
SS_EXTENSIONS = {".csv", ".xlsx", ".xls"}   # → SQL Server
PG_EXTENSIONS = {".json", ".txt"}            # → PostgreSQL


# ── SQL Server connection builders
def get_ss_connection_string():
    conn = f"DRIVER={{{SS_ODBC}}};SERVER={SS_SERVER};DATABASE={SS_DATABASE};Trusted_Connection=yes;TrustServerCertificate=yes"
    return f"mssql+pyodbc:///?odbc_connect={conn}"


def get_ss_master_connection_string():
    conn = f"DRIVER={{{SS_ODBC}}};SERVER={SS_SERVER};DATABASE=master;Trusted_Connection=yes;TrustServerCertificate=yes"
    return f"mssql+pyodbc:///?odbc_connect={conn}"


# ── PostgreSQL connection builders
def get_pg_connection_string():
    pwd = quote_plus(PG_PASSWORD)
    return (
        f"postgresql+psycopg2://{PG_USERNAME}:{pwd}"
        f"@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    )


def get_pg_master_connection_string():
    pwd = quote_plus(PG_PASSWORD)
    return (
        f"postgresql+psycopg2://{PG_USERNAME}:{pwd}"
        f"@{PG_HOST}:{PG_PORT}/postgres"
    )