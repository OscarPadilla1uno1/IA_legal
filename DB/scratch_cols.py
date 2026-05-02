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

cursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'fragmentos_texto';")
print(cursor.fetchall())
