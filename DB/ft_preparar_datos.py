"""
ft_preparar_datos.py
Extrae pares (anchor, positive) balanceados por clase desde PostgreSQL.
Guarda ft_datos_train.jsonl y ft_datos_val.jsonl listos para el Trainer.
"""
import sys, json, random, time
sys.stdout.reconfigure(encoding="utf-8")
import psycopg2

DB = {"dbname":"legal_ia","user":"root","password":"rootpassword","host":"localhost","port":"5432"}

# Pares por clase — más peso a las clases pequeñas
PLAN = {
    "ADMITIDO":       {"train": 10000, "val": 1000},
    "DENEGADO":       {"train": 10000, "val": 1000},
    "CASACION":       {"train":  8000, "val":  800},
    "SOBRESEIMIENTO": {"train":  6000, "val":  600},
    "NULIDAD":        {"train":  4000, "val":  400},
    "OTRO":           {"train":  2000, "val":  200},
}

MIN_CHARS = 80    # fragmentos muy cortos aportan ruido
MAX_CHARS = 512   # truncar para balance con max_seq_length

random.seed(42)
conn = psycopg2.connect(**DB)
cur  = conn.cursor()

pares_train, pares_val = [], []

print("Extrayendo pares de entrenamiento por clase...\n")
for clase, cfg in PLAN.items():
    t0 = time.time()
    needed_total = cfg["train"] + cfg["val"]
    # Necesitamos el doble de fragmentos (anchor + positive por par)
    n_fetch = needed_total * 2 + 500  # margen por filtrado

    cur.execute("""
        SELECT ft.contenido
        FROM fragmentos_texto ft
        JOIN sentencias s ON s.id = ft.sentencia_id
        WHERE s.fallo_macro = %s
          AND LENGTH(ft.contenido) >= %s
          AND ft.embedding_fragmento IS NOT NULL
        ORDER BY RANDOM()
        LIMIT %s;
    """, (clase, MIN_CHARS, n_fetch))

    textos = [r[0][:MAX_CHARS].strip() for r in cur.fetchall()]
    textos = [t for t in textos if len(t) >= MIN_CHARS]

    # Shuffle y emparejar: cada par de fragmentos = (anchor, positive)
    random.shuffle(textos)
    pares = []
    for i in range(0, len(textos) - 1, 2):
        pares.append({"anchor": textos[i], "positive": textos[i+1], "clase": clase})

    if len(pares) < cfg["train"] + cfg["val"]:
        print(f"  ⚠️  {clase}: solo {len(pares)} pares (pedido: {cfg['train']+cfg['val']})")

    # Split train/val
    n_train = min(cfg["train"], len(pares))
    n_val   = min(cfg["val"], len(pares) - n_train)

    pares_train.extend(pares[:n_train])
    pares_val.extend(pares[n_train:n_train + n_val])

    elapsed = time.time() - t0
    print(f"  {clase:<18}: {n_train:>5} train + {n_val:>4} val  ({elapsed:.1f}s)")

cur.close()
conn.close()

# Shuffle global
random.shuffle(pares_train)
random.shuffle(pares_val)

# Guardar como JSONL
out_train = "DB/ft_datos_train.jsonl"
out_val   = "DB/ft_datos_val.jsonl"

with open(out_train, "w", encoding="utf-8") as f:
    for p in pares_train:
        f.write(json.dumps(p, ensure_ascii=False) + "\n")

with open(out_val, "w", encoding="utf-8") as f:
    for p in pares_val:
        f.write(json.dumps(p, ensure_ascii=False) + "\n")

print(f"\n✅ Train: {len(pares_train):,} pares  → {out_train}")
print(f"✅ Val:   {len(pares_val):,} pares  → {out_val}")

# Distribución final
from collections import Counter
dist_train = Counter(p["clase"] for p in pares_train)
print("\nDistribución train:")
for clase, n in sorted(dist_train.items(), key=lambda x: -x[1]):
    print(f"  {clase:<18} {n:>6,}  ({n/len(pares_train)*100:.1f}%)")
