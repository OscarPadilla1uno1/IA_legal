import json, psycopg2

with open(r"D:\CodigosHN_v2\codigos_descargados.json", encoding="utf-8") as f:
    manifest = json.load(f)

DB = {"dbname":"legal_ia","user":"root","password":"rootpassword","host":"localhost","port":"5432"}
conn = psycopg2.connect(**DB)
cur = conn.cursor()

cur.execute("SELECT nombre_oficial, metadata FROM codigos_honduras ORDER BY created_at LIMIT 10;")
print("=== BD (primeras 10 filas) ===")
for r in cur.fetchall():
    print(f"  nombre  : {str(r[0])[:70]}")
    print(f"  metadata: {r[1]}")
    print()

print("=== Manifest (primeras 5 entradas) ===")
for e in manifest[:5]:
    print(f"  titulo  : {e['titulo'][:70]}")
    print(f"  archivo : {e['archivo']}")
    print()

cur.close()
conn.close()
