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

cursor.execute("SELECT count(*) FROM sentencias WHERE embedding IS NOT NULL;")
cnt_notnull = cursor.fetchone()[0]

cursor.execute("SELECT count(*) FROM sentencias WHERE embedding IS NULL;")
cnt_null = cursor.fetchone()[0]

print(f"Sentencias NOT NULL embeddings: {cnt_notnull}")
print(f"Sentencias NULL embeddings: {cnt_null}")
