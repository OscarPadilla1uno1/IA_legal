import psycopg2

DB = {"dbname":"legal_ia","user":"root","password":"rootpassword","host":"localhost","port":"5432"}
conn = psycopg2.connect(**DB)
cur = conn.cursor()

print("\n--- CONTEO GENERAL ---")
for t in ["codigos_honduras","capitulos_codigo","articulos_codigo","gacetas_oficiales"]:
    cur.execute(f"SELECT COUNT(*) FROM {t};")
    n = cur.fetchone()[0]
    print(f"  {t:<30} {n:>8,}")

print("\n--- CODIGOS CARGADOS ---")
cur.execute("SELECT nombre_oficial, COUNT(cap.id) as caps, COUNT(art.id) as arts FROM codigos_honduras c LEFT JOIN capitulos_codigo cap ON cap.codigo_id = c.id LEFT JOIN articulos_codigo art ON art.codigo_id = c.id GROUP BY c.id, c.nombre_oficial ORDER BY c.nombre_oficial;")
rows = cur.fetchall()
for r in rows:
    print(f"  {str(r[0])[:60]:<60}  caps: {r[1]:>5,}  arts: {r[2]:>6,}")

print("\n--- VECTORIZACION CODIGOS ---")
for t, col in [("capitulos_codigo","embedding"),("articulos_codigo","embedding"),("gacetas_oficiales","embedding")]:
    cur.execute(f"SELECT COUNT(*) FROM {t} WHERE {col} IS NOT NULL;")
    ok = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM {t} WHERE {col} IS NULL;")
    null = cur.fetchone()[0]
    total = ok + null
    pct = (ok/total*100) if total else 0
    print(f"  {t}.{col}: {ok:,}/{total:,} ({pct:.1f}%)")

cur.close()
conn.close()
