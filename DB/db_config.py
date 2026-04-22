import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent

# Carga primero el .env de la raiz y luego el de DB para permitir overrides locales.
load_dotenv(PROJECT_ROOT / ".env")
load_dotenv(BASE_DIR / ".env", override=True)


def get_db_connect_kwargs():
    database_url = os.getenv("DATABASE_URL", "").strip()
    if database_url:
        return {"dsn": database_url}

    password = os.getenv("DB_PASSWORD")
    if password is None:
        password = os.getenv("PGPASSWORD")
    if password is None:
        password = "rootpassword"

    return {
        "dbname": os.getenv("DB_NAME") or os.getenv("PGDATABASE") or "legal_ia",
        "user": os.getenv("DB_USER") or os.getenv("PGUSER") or "root",
        "password": password,
        "host": os.getenv("DB_HOST") or os.getenv("PGHOST") or "localhost",
        "port": os.getenv("DB_PORT") or os.getenv("PGPORT") or "5432",
    }


def connect_db():
    return psycopg2.connect(**get_db_connect_kwargs())


def describe_db_target():
    params = get_db_connect_kwargs()
    if "dsn" in params:
        return "DATABASE_URL"
    return f"{params['user']}@{params['host']}:{params['port']}/{params['dbname']}"
