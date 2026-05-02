import psycopg2

DB = {"dbname":"legal_ia","user":"root","password":"rootpassword","host":"localhost","port":"5432"}
conn = psycopg2.connect(**DB)
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM codigos_honduras;")
total = cur.fetchone()[0]

# Vacíos = sin capítulos ni artículos
cur.execute("""
    SELECT COUNT(*) FROM codigos_honduras c
    WHERE NOT EXISTS (SELECT 1 FROM capitulos_codigo WHERE codigo_id = c.id)
      AND NOT EXISTS (SELECT 1 FROM articulos_codigo WHERE codigo_id = c.id);
""")
vacios = cur.fetchone()[0]

# Con contenido
con_contenido = total - vacios

print(f"Total documentos en codigos_honduras : {total:>6,}")
print(f"  Con contenido (texto extraído)     : {con_contenido:>6,}")
print(f"  Vacíos (probablemente imagen/OCR)  : {vacios:>6,}")
print(f"  % que necesita OCR                 : {vacios/total*100:.1f}%")

print("\n--- Top documentos vacíos (primeros 20) ---")
cur.execute("""
    SELECT c.nombre_oficial FROM codigos_honduras c
    WHERE NOT EXISTS (SELECT 1 FROM capitulos_codigo WHERE codigo_id = c.id)
    ORDER BY c.nombre_oficial
    LIMIT 20;
""")
for r in cur.fetchall():
    print(f"  {r[0]}")

cur.close()
conn.close()
