import psycopg2
import datetime

DB = {"dbname":"legal_ia","user":"root","password":"rootpassword","host":"localhost","port":"5432"}
conn = psycopg2.connect(**DB)
cur = conn.cursor()

print(f"\n{'='*50}")
print(f"  ESTADO DE VECTORIZACIÓN")
print(f"  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*50}")

checks = [
    ("fragmentos_texto",  "embedding_fragmento", "Fragmentos de sentencias"),
    ("sentencias",        "embedding",           "Sentencias íntegras"),
]

todo_listo = True
for tabla, col, label in checks:
    cur.execute(f"SELECT COUNT(*) FROM {tabla} WHERE {col} IS NOT NULL;")
    ok = cur.fetchone()[0]
    cur.execute(f"SELECT COUNT(*) FROM {tabla} WHERE {col} IS NULL;")
    null = cur.fetchone()[0]
    total = ok + null
    pct = (ok / total * 100) if total else 0
    barra = int(pct / 5)
    barra_str = "█" * barra + "░" * (20 - barra)

    estado = "✅ COMPLETO" if null == 0 else "⏳ EN PROGRESO"
    if null > 0:
        todo_listo = False

    print(f"\n  {label}")
    print(f"  [{barra_str}] {pct:.1f}%")
    print(f"  {ok:,} vectorizados | {null:,} pendientes  {estado}")

print(f"\n{'='*50}")
if todo_listo:
    print("  🎉 TODO VECTORIZADO — el sistema está listo.")
else:
    print("  ⚙️  Proceso en curso — vuelve a ejecutar para ver el avance.")
print(f"{'='*50}\n")

cur.close()
conn.close()
