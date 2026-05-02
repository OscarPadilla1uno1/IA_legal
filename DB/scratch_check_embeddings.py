import psycopg2

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432"
}

conn = psycopg2.connect(**DB_PARAMS)
cursor = conn.cursor()

cursor.execute("SELECT count(*) FROM fragmentos_texto WHERE embedding_fragmento IS NOT NULL;")
cnt_notnull = cursor.fetchone()[0]

cursor.execute("SELECT count(*) FROM fragmentos_texto WHERE embedding_fragmento IS NULL;")
cnt_null = cursor.fetchone()[0]

print(f"NOT NULL embeddings: {cnt_notnull}")
print(f"NULL embeddings: {cnt_null}")
