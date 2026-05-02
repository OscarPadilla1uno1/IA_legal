import psycopg2

DB = {"dbname":"legal_ia","user":"root","password":"rootpassword","host":"localhost","port":"5432"}
conn = psycopg2.connect(**DB)
cur = conn.cursor()

# Ver si hay columna de ruta de archivo
cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'codigos_honduras'
    ORDER BY ordinal_position;
""")
print("Columnas de codigos_honduras:")
for r in cur.fetchall():
    print(f"  {r[0]}")

# Ver muestra de datos de la tabla
print("\nMuestra de registros:")
cur.execute("SELECT * FROM codigos_honduras LIMIT 5;")
rows = cur.fetchall()
for r in rows:
    print(f"  {r}")

# Ver si hay metadata con ruta
print("\nMetadata de primeros 5:")
cur.execute("SELECT nombre_oficial, metadata FROM codigos_honduras LIMIT 5;")
for r in cur.fetchall():
    print(f"  nombre: {r[0]}")
    print(f"  metadata: {r[1]}")
    print()

cur.close()
conn.close()
