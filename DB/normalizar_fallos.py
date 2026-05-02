"""
PASO 1 — Normalización de etiquetas
Consolida 81 valores de 'fallo' → 6 macro-categorías.
Añade columna 'fallo_macro' a la tabla sentencias.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")
import psycopg2

DB = {"dbname":"legal_ia","user":"root","password":"rootpassword","host":"localhost","port":"5432"}

# ── Mapa de normalización ────────────────────────────────────────
# Orden importante: primero los más específicos
MAPA = {
    "ADMITIDO": [
        "con lugar", "ha lugar", "ha lugar parcialmente", "otorgado",
        "otorgado parcialmente", "reforma otorgamiento", "confirmando otorgamiento",
        "revoca denegatoria", "declara la procedencia", "declara competente",
        "se declara competente", "admitida a trámite", "auto de admisión",
        "fundada la queja", "reconocer y darle cumplimiento",
        "declarar inconstitucional", "inconstitucionalidad",
        "inconstitucionalidad parcial", "declara la constitucionalidad",
    ],
    "DENEGADO": [
        "no ha lugar", "sin lugar", "denegado", "no admitida", "no admitido",
        "auto de inadmisión", "inadmisibilidad", "inadmisión in limine",
        "no admitida", "declara la no procedencia", "infundada la queja",
        "improcedente la acción", "no ha lugar la admisión", "rechazada",
        "desestima", "confirma inadmisibilidad", "recurso no admitido",
        "confirmando denegatoria", "reforma denegatoria",
    ],
    "SOBRESEIMIENTO": [
        "sobreseimiento", "confirmando sobreseimiento", "sobreseer",
        "reformando sobreseimiento", "revoca sobreseimiento",
    ],
    "NULIDAD": [
        "nulidad absoluta", "nulidad de oficio", "nulidad subsidiaria",
        "nulidad de autos",
    ],
    "CASACION": [
        "casa totalmente", "casa parcialmente", "revoca fallo",
        "reforma fallo", "revoca otorgamiento", "confirma fallo",
        "confirma parcialmente", "confirma inadmisibilidad",
    ],
    "OTRO": [
        "desistimiento", "reserva de actuaciones", "reservado",
        "suspensión de la ciudadanía", "amonestación", "precluído",
        "desierto", "abstenerse", "devolver antecedentes", "retirada",
        "libramiento de comunicación", "declara de oficio", "verificar",
    ],
}

def clasificar_fallo(fallo_raw):
    if not fallo_raw:
        return "DESCONOCIDO"
    fallo_l = fallo_raw.strip().lower()
    for macro, patrones in MAPA.items():
        for p in patrones:
            if fallo_l.startswith(p) or p in fallo_l:
                return macro
    return "OTRO"

conn = psycopg2.connect(**DB)
cur = conn.cursor()

# Agregar columna si no existe
print("Verificando/creando columna fallo_macro...")
cur.execute("""
    ALTER TABLE sentencias
    ADD COLUMN IF NOT EXISTS fallo_macro TEXT;
""")
conn.commit()

# Leer todos los fallos
cur.execute("SELECT id, fallo FROM sentencias WHERE fallo IS NOT NULL;")
rows = cur.fetchall()
print(f"Normalizando {len(rows):,} sentencias...")

updates = []
for sid, fallo in rows:
    macro = clasificar_fallo(fallo)
    updates.append((macro, str(sid)))

# Batch update
from psycopg2.extras import execute_batch
execute_batch(cur, "UPDATE sentencias SET fallo_macro = %s WHERE id = %s::uuid;",
              updates, page_size=500)
conn.commit()

# Reporte
cur.execute("""
    SELECT fallo_macro, COUNT(*) as n
    FROM sentencias
    WHERE fallo_macro IS NOT NULL
    GROUP BY fallo_macro ORDER BY n DESC;
""")
resultados = cur.fetchall()
total = sum(r[1] for r in resultados)

print("\n=== DISTRIBUCIÓN MACRO-CLASES ===")
for macro, n in resultados:
    bar = "█" * int(n / total * 40)
    print(f"  {macro:<18} {n:>6,} ({n/total*100:5.1f}%)  {bar}")
print(f"\n  Total: {total:,} sentencias normalizadas")
print(f"  Clases: {len(resultados)}")

# Verificar balance para ML
print("\n=== VIABILIDAD ML ===")
min_clase = min(r[1] for r in resultados)
max_clase = max(r[1] for r in resultados)
ratio = max_clase / min_clase
if ratio < 10:
    print(f"  Balance OK (ratio max/min = {ratio:.1f}x)")
elif ratio < 30:
    print(f"  Desbalance moderado (ratio {ratio:.1f}x) — usar class_weight='balanced'")
else:
    print(f"  Desbalance severo (ratio {ratio:.1f}x) — considerar oversample de clases pequeñas")

cur.close()
conn.close()
print("\nNormalización completada.")
