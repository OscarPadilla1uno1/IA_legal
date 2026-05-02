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

tables_to_check = [
    ("articulos_codigo", "embedding"),
    ("capitulos_codigo", "embedding"),
    ("gacetas_oficiales", "embedding"),
    ("fragmentos_normativos", "embedding_fragmento"),
    ("tesauro_juridico", "embedding")
]

print("--- RECUENTO DE VECTORES EN DB ---")
for table, col in tables_to_check:
    try:
        cursor.execute(f"SELECT count(*) FROM {table} WHERE {col} IS NOT NULL;")
        count = cursor.fetchone()[0]
        print(f"Tabla '{table}': {count} vectores")
    except Exception as e:
        conn.rollback()

cursor.close()
conn.close()
