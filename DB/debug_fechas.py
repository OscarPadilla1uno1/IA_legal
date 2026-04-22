import re
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

# Ver 20 ejemplos de nombres de sentencia para entender el patrón
cursor.execute("SELECT numero_sentencia FROM sentencias WHERE fecha_resolucion IS NULL LIMIT 20;")
for row in cursor.fetchall():
    print(row[0])

cursor.close()
conn.close()
