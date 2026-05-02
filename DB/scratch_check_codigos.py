import psycopg2

DB = {"dbname":"legal_ia","user":"root","password":"rootpassword","host":"localhost","port":"5432"}
conn = psycopg2.connect(**DB)
cur = conn.cursor()

tablas = ["codigos_honduras","capitulos_codigo","articulos_codigo","gacetas_oficiales"]
print(f'\n{"TABLA":<30} {"FILAS":>10}')
print("="*42)
for t in tablas:
    try:
        cur.execute(f"SELECT COUNT(*) FROM {t};")
        n = cur.fetchone()[0]
        print(f"{t:<30} {n:>10,}")
    except Exception:
        print(f"{t:<30} {'(no existe)':>10}")
        conn.rollback()

# Detalle de codigos
print()
try:
    cur.execute("SELECT nombre_oficial, fecha_promulgacion FROM codigos_honduras;")
    rows = cur.fetchall()
    if rows:
        print("Codigos cargados:")
        for r in rows:
            print(f"  - {r[0]} (promulgado: {r[1]})")
    else:
        print("codigos_honduras: tabla vacia")
except Exception as e:
    print(f"Error: {e}")
    conn.rollback()

# Estado vectorizacion
print()
for t, col in [("capitulos_codigo","embedding"),("articulos_codigo","embedding"),("gacetas_oficiales","embedding")]:
    try:
        cur.execute(f"SELECT COUNT(*) FROM {t} WHERE {col} IS NOT NULL;")
        ok = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*) FROM {t} WHERE {col} IS NULL;")
        null = cur.fetchone()[0]
        total = ok + null
        pct = (ok/total*100) if total else 0
        print(f"{t}.{col}: {ok:,}/{total:,} ({pct:.1f}%)")
    except Exception as e:
        print(f"Error {t}: {e}")
        conn.rollback()

cur.close()
conn.close()
