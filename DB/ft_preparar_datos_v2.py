"""
ft_preparar_datos_v2.py
Prepara datos en formato (texto, etiqueta_id) para BatchHardTripletLoss.
Esto evita el mode collapse al entender clases reales.
"""
import sys, json, random, time
sys.stdout.reconfigure(encoding="utf-8")
import psycopg2

DB = {"dbname":"legal_ia","user":"root","password":"rootpassword","host":"localhost","port":"5432"}

# Muestra más pequeña para rapidez (10k total)
PLAN = {
    "ADMITIDO":       2500,
    "DENEGADO":       2500,
    "CASACION":       2000,
    "SOBRESEIMIENTO": 1500,
    "NULIDAD":        1000,
    "OTRO":            500,
}

MIN_CHARS = 100
MAX_CHARS = 512

# Mapeo de clases a IDs numéricos
CLASE_TO_ID = {
    "ADMITIDO": 0,
    "DENEGADO": 1,
    "CASACION": 2,
    "SOBRESEIMIENTO": 3,
    "NULIDAD": 4,
    "OTRO": 5
}

random.seed(42)
conn = psycopg2.connect(**DB)
cur  = conn.cursor()

dataset = []

print("Extrayendo fragmentos etiquetados...\n")
for clase, n_needed in PLAN.items():
    t0 = time.time()
    cur.execute("""
        SELECT ft.contenido
        FROM fragmentos_texto ft
        JOIN sentencias s ON s.id = ft.sentencia_id
        WHERE s.fallo_macro = %s
          AND LENGTH(ft.contenido) >= %s
        ORDER BY RANDOM()
        LIMIT %s;
    """, (clase, MIN_CHARS, n_needed))

    rows = cur.fetchall()
    for r in rows:
        dataset.append({
            "text": r[0][:MAX_CHARS].strip(),
            "label": CLASE_TO_ID[clase]
        })
    
    print(f"  {clase:<18}: {len(rows):>5} fragmentos ({time.time()-t0:.1f}s)")

cur.close()
conn.close()

random.shuffle(dataset)

# Split 90/10
split = int(len(dataset) * 0.9)
train_ds = dataset[:split]
val_ds   = dataset[split:]

out_train = "DB/ft_v2_train.jsonl"
out_val   = "DB/ft_v2_val.jsonl"

with open(out_train, "w", encoding="utf-8") as f:
    for item in train_ds:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")

with open(out_val, "w", encoding="utf-8") as f:
    for item in val_ds:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")

print(f"\n✅ Dataset V2 listo:")
print(f"   Train: {len(train_ds):,} items")
print(f"   Val:   {len(val_ds):,} items")
