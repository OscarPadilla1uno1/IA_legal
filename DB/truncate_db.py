import psycopg2

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432"
}

try:
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    cursor.execute("TRUNCATE codigos_honduras, capitulos_codigo, articulos_codigo, gacetas_oficiales CASCADE;")
    conn.commit()
    print("Tablas reseteadas exitosamente.")
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error: {e}")
