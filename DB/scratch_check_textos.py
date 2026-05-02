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

cursor.execute("SELECT count(*) FROM sentencias WHERE texto_integro IS NOT NULL AND length(texto_integro) > 10;")
cnt_notnull = cursor.fetchone()[0]

print(f"Sentencias con texto íntegro: {cnt_notnull}")
cursor.close()
conn.close()
