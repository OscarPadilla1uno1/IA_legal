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

tablas = [
    "sentencias",
    "fragmentos_texto",
    "tipos_proceso",
    "magistrados",
    "tribunales",
    "materias",
    "personas_entidades",
    "tesauro",
    "legislacion",
    "sentencias_materias",
    "sentencias_legislacion",
    "documentos_pdf",
    "leyes",
    "leyes_alias",
    "leyes_versiones",
    "articulos_ley",
    "nodos_normativos",
    "legislaciones",
    "fragmentos_normativos",
    "norma_fragmentos",
]

print(f"\n{'TABLA':<35} {'FILAS':>10}")
print("=" * 50)
total_filas = 0
for tabla in tablas:
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {tabla};")
        n = cursor.fetchone()[0]
        total_filas += n
        print(f"{tabla:<35} {n:>10,}")
    except Exception:
        print(f"{tabla:<35} {'(no existe)':>10}")

print("=" * 50)
print(f"{'TOTAL FILAS':<35} {total_filas:>10,}")

# Fragmentos con y sin embedding
print("\n--- Vectorización ---")
for tabla, col in [("fragmentos_texto", "embedding_fragmento"), ("sentencias", "embedding"), ("norma_fragmentos", "embedding_fragmento")]:
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {tabla} WHERE {col} IS NOT NULL;")
        ok = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM {tabla} WHERE {col} IS NULL;")
        null = cursor.fetchone()[0]
        total = ok + null
        pct = (ok/total*100) if total else 0
        print(f"  {tabla}.{col}: {ok:,} / {total:,}  ({pct:.1f}%)")
    except Exception:
        pass

# Distribución de tipos de fragmento
print("\n--- Tipos de fragmento ---")
try:
    cursor.execute("SELECT tipo_fragmento, COUNT(*) FROM fragmentos_texto GROUP BY tipo_fragmento ORDER BY COUNT(*) DESC;")
    for row in cursor.fetchall():
        print(f"  {str(row[0]):<30} {row[1]:>10,}")
except Exception as e:
    print(f"  Error: {e}")

# Top 10 tribunales
print("\n--- Top 10 Tribunales ---")
try:
    cursor.execute("""
        SELECT t.nombre, COUNT(s.id) as total
        FROM sentencias s JOIN tribunales t ON s.tribunal_id = t.id
        GROUP BY t.nombre ORDER BY total DESC LIMIT 10;
    """)
    for row in cursor.fetchall():
        print(f"  {str(row[0])[:45]:<45} {row[1]:>6,}")
except Exception as e:
    print(f"  Error: {e}")

# Top 10 magistrados
print("\n--- Top 10 Magistrados ---")
try:
    cursor.execute("""
        SELECT m.nombre, COUNT(s.id) as total
        FROM sentencias s JOIN magistrados m ON s.magistrado_id = m.id
        GROUP BY m.nombre ORDER BY total DESC LIMIT 10;
    """)
    for row in cursor.fetchall():
        print(f"  {str(row[0])[:45]:<45} {row[1]:>6,}")
except Exception as e:
    print(f"  Error: {e}")

# Sentencias por año
print("\n--- Sentencias por Año ---")
try:
    cursor.execute("""
        SELECT EXTRACT(YEAR FROM fecha_resolucion)::int AS año, COUNT(*) as total
        FROM sentencias
        WHERE fecha_resolucion IS NOT NULL
        GROUP BY año ORDER BY año;
    """)
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]:,}")
except Exception as e:
    print(f"  Error: {e}")

cursor.close()
conn.close()
