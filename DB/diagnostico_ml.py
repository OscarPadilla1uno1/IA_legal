"""
==============================================================
  DIAGNÓSTICO ML - Base de Datos Legal Honduras
  Evalúa si la data está óptima para Machine Learning
==============================================================
"""
import sys
import json
import math
import statistics
sys.stdout.reconfigure(encoding="utf-8")

import psycopg2
import datetime

DB = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432",
}

VERDE  = "\033[92m"
AMARILLO = "\033[93m"
ROJO   = "\033[91m"
AZUL   = "\033[94m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def color_pct(pct, umbral_ok=90, umbral_warn=70):
    if pct >= umbral_ok:
        return f"{VERDE}{pct:.1f}%{RESET}"
    elif pct >= umbral_warn:
        return f"{AMARILLO}{pct:.1f}%{RESET}"
    else:
        return f"{ROJO}{pct:.1f}%{RESET}"

def barra(pct, ancho=30):
    llenos = int(pct / 100 * ancho)
    return "█" * llenos + "░" * (ancho - llenos)

def separador(titulo=""):
    if titulo:
        print(f"\n{BOLD}{AZUL}{'─'*60}{RESET}")
        print(f"{BOLD}{AZUL}  {titulo}{RESET}")
        print(f"{BOLD}{AZUL}{'─'*60}{RESET}")
    else:
        print(f"{AZUL}{'─'*60}{RESET}")

def score_label(score):
    if score >= 90:
        return f"{VERDE}EXCELENTE ✅{RESET}"
    elif score >= 75:
        return f"{VERDE}BUENO ✅{RESET}"
    elif score >= 55:
        return f"{AMARILLO}ACEPTABLE ⚠️{RESET}"
    else:
        return f"{ROJO}DEFICIENTE ❌{RESET}"

conn = psycopg2.connect(**DB)
cur = conn.cursor()

print(f"\n{BOLD}{'='*60}{RESET}")
print(f"{BOLD}  DIAGNÓSTICO ML — BASE LEGAL HONDURAS{RESET}")
print(f"  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{BOLD}{'='*60}{RESET}")

scores = {}

# ══════════════════════════════════════════════════════════════
# 1. COBERTURA VECTORIAL
# ══════════════════════════════════════════════════════════════
separador("1. COBERTURA VECTORIAL (Embeddings)")

cur.execute("SELECT COUNT(*) FROM fragmentos_texto;")
total_frag = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM fragmentos_texto WHERE embedding_fragmento IS NOT NULL;")
frag_ok = cur.fetchone()[0]
pct_frag = frag_ok / total_frag * 100 if total_frag else 0

cur.execute("SELECT COUNT(*) FROM sentencias;")
total_sent = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM sentencias WHERE embedding IS NOT NULL;")
sent_ok = cur.fetchone()[0]
pct_sent = sent_ok / total_sent * 100 if total_sent else 0

print(f"\n  Fragmentos de texto:")
print(f"  [{barra(pct_frag)}] {color_pct(pct_frag)}  ({frag_ok:,} / {total_frag:,})")
print(f"\n  Sentencias íntegras:")
print(f"  [{barra(pct_sent)}] {color_pct(pct_sent)}  ({sent_ok:,} / {total_sent:,})")

score_cobertura = (pct_frag * 0.6 + pct_sent * 0.4)
scores["Cobertura Vectorial"] = score_cobertura
print(f"\n  Score cobertura: {score_label(score_cobertura)}")

# ══════════════════════════════════════════════════════════════
# 2. CALIDAD TEXTUAL (fragmentos)
# ══════════════════════════════════════════════════════════════
separador("2. CALIDAD TEXTUAL")

cur.execute("SELECT COUNT(*) FROM fragmentos_texto WHERE contenido IS NULL OR TRIM(contenido) = '';")
frag_vacios = cur.fetchone()[0]
pct_vacios = frag_vacios / total_frag * 100 if total_frag else 0

cur.execute("SELECT AVG(LENGTH(contenido)), MIN(LENGTH(contenido)), MAX(LENGTH(contenido)), PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY LENGTH(contenido)) FROM fragmentos_texto WHERE contenido IS NOT NULL;")
avg_len, min_len, max_len, median_len = cur.fetchone()
avg_len = float(avg_len) if avg_len else 0
median_len = float(median_len) if median_len else 0

cur.execute("SELECT COUNT(*) FROM fragmentos_texto WHERE LENGTH(contenido) < 50;")
frag_muy_cortos = cur.fetchone()[0]
pct_cortos = frag_muy_cortos / total_frag * 100 if total_frag else 0

cur.execute("SELECT COUNT(*) FROM sentencias WHERE texto_integro IS NULL OR TRIM(texto_integro) = '';")
sent_sin_texto = cur.fetchone()[0]
pct_sent_vacias = sent_sin_texto / total_sent * 100 if total_sent else 0

cur.execute("SELECT AVG(LENGTH(texto_integro)) FROM sentencias WHERE texto_integro IS NOT NULL;")
avg_sent_len = float(cur.fetchone()[0] or 0)

print(f"\n  Fragmentos vacíos/nulos     : {frag_vacios:,}  ({color_pct(100-pct_vacios)} completos)")
print(f"  Fragmentos < 50 chars       : {frag_muy_cortos:,}  ({pct_cortos:.1f}%)")
print(f"  Longitud media fragmento    : {avg_len:,.0f} chars | Mediana: {median_len:,.0f}")
print(f"  Longitud min/max fragmento  : {min_len:,} / {max_len:,}")
print(f"\n  Sentencias sin texto íntegro: {sent_sin_texto:,}  ({color_pct(100-pct_sent_vacias)} completas)")
print(f"  Longitud media sentencia    : {avg_sent_len:,.0f} chars (~{avg_sent_len/5:.0f} palabras)")

score_texto = 100 - (pct_vacios * 2) - (pct_cortos * 0.5) - (pct_sent_vacias * 3)
score_texto = max(0, min(100, score_texto))
scores["Calidad Textual"] = score_texto
print(f"\n  Score calidad textual: {score_label(score_texto)}")

# ══════════════════════════════════════════════════════════════
# 3. BALANCE DE CLASES (Distribución por categorías)
# ══════════════════════════════════════════════════════════════
separador("3. BALANCE DE CLASES")

# Tipos de fragmento
cur.execute("""
    SELECT tipo_fragmento, COUNT(*) as n
    FROM fragmentos_texto
    GROUP BY tipo_fragmento
    ORDER BY n DESC;
""")
tipos_frag = cur.fetchall()

print(f"\n  Distribución por tipo de fragmento:")
total_tipos = sum(r[1] for r in tipos_frag)
for tipo, n in tipos_frag:
    pct = n / total_tipos * 100 if total_tipos else 0
    etiqueta = tipo or "NULL"
    print(f"    {etiqueta:<30} {n:>8,}  ({pct:5.1f}%)  [{barra(pct, 20)}]")

# Fallos (etiquetas para clasificación)
cur.execute("""
    SELECT fallo, COUNT(*) as n
    FROM sentencias
    WHERE fallo IS NOT NULL AND TRIM(fallo) != ''
    GROUP BY fallo
    ORDER BY n DESC
    LIMIT 15;
""")
fallos = cur.fetchall()

cur.execute("SELECT COUNT(*) FROM sentencias WHERE fallo IS NOT NULL AND TRIM(fallo) != '';")
total_con_fallo = cur.fetchone()[0]
pct_con_fallo = total_con_fallo / total_sent * 100 if total_sent else 0

print(f"\n  Sentencias con etiqueta 'fallo' (label para clasificación):")
print(f"  Total con fallo: {total_con_fallo:,} / {total_sent:,}  ({color_pct(pct_con_fallo)})")
print(f"\n  Top 15 valores de 'fallo':")
for fallo_val, n in fallos:
    pct = n / total_con_fallo * 100 if total_con_fallo else 0
    print(f"    {str(fallo_val)[:35]:<37} {n:>7,}  ({pct:5.1f}%)")

# Cálculo de entropía para balance
if fallos:
    total_f = sum(r[1] for r in fallos)
    probs = [r[1] / total_f for r in fallos]
    entropia = -sum(p * math.log2(p) for p in probs if p > 0)
    max_entropia = math.log2(len(fallos))
    balance_pct = (entropia / max_entropia * 100) if max_entropia > 0 else 0
    print(f"\n  Entropía de distribución: {entropia:.2f} bits (max: {max_entropia:.2f})")
    print(f"  Balance de clases: {color_pct(balance_pct, umbral_ok=70, umbral_warn=40)}")
    score_balance = balance_pct
else:
    score_balance = 0
    balance_pct = 0
    print(f"\n  {ROJO}Sin etiquetas de fallo — no se puede evaluar balance.{RESET}")

scores["Balance de Clases"] = score_balance

# ══════════════════════════════════════════════════════════════
# 4. COMPLETITUD DE METADATOS (Features para ML)
# ══════════════════════════════════════════════════════════════
separador("4. COMPLETITUD DE METADATOS (Features)")

campos = [
    ("numero_sentencia",          "Número de sentencia"),
    ("fecha_resolucion",          "Fecha resolución"),
    ("fallo",                     "Fallo/resultado"),
    ("tipo_proceso_id",           "Tipo de proceso"),
    ("magistrado_id",             "Magistrado"),
    ("tribunal_id",               "Tribunal"),
]

print(f"\n  {'Campo':<35} {'Completo':>10}  {'%':>8}")
print(f"  {'─'*55}")

pcts_meta = []
for campo, label in campos:
    cur.execute(f"SELECT COUNT(*) FROM sentencias WHERE {campo} IS NOT NULL;")
    n_ok = cur.fetchone()[0]
    pct = n_ok / total_sent * 100 if total_sent else 0
    pcts_meta.append(pct)
    indicador = color_pct(pct)
    print(f"  {label:<35} {n_ok:>10,}  {indicador:>8}")

# Materias (N:M)
cur.execute("SELECT COUNT(DISTINCT sentencia_id) FROM sentencias_materias;")
sent_con_materia = cur.fetchone()[0]
pct_materia = sent_con_materia / total_sent * 100 if total_sent else 0
pcts_meta.append(pct_materia)
print(f"  {'Materia jurídica (N:M)':<35} {sent_con_materia:>10,}  {color_pct(pct_materia):>8}")

# Tesauro
cur.execute("SELECT COUNT(DISTINCT sentencia_id) FROM tesauro;")
sent_con_tesauro = cur.fetchone()[0]
pct_tesauro = sent_con_tesauro / total_sent * 100 if total_sent else 0
pcts_meta.append(pct_tesauro)
print(f"  {'Tesauro (categ. jurídica)':<35} {sent_con_tesauro:>10,}  {color_pct(pct_tesauro):>8}")

score_meta = statistics.mean(pcts_meta)
scores["Completitud Metadatos"] = score_meta
print(f"\n  Score metadatos: {score_label(score_meta)}")

# ══════════════════════════════════════════════════════════════
# 5. VOLUMEN Y ESCALA
# ══════════════════════════════════════════════════════════════
separador("5. VOLUMEN Y ESCALA (suficiencia para ML)")

print(f"\n  {'Tabla':<35} {'Registros':>12}")
print(f"  {'─'*50}")

tablas = [
    ("sentencias",              "Sentencias judiciales"),
    ("fragmentos_texto",        "Fragmentos (chunks RAG)"),
    ("materias",                "Materias jurídicas"),
    ("magistrados",             "Magistrados"),
    ("tribunales",              "Tribunales"),
    ("tipos_proceso",           "Tipos de proceso"),
    ("tesauro",                 "Entradas tesauro"),
    ("legislacion",             "Referencias legislativas"),
    ("leyes",                   "Leyes normativas"),
    ("articulos_ley",           "Artículos de ley"),
]

volumen_total = 0
for tabla, label in tablas:
    try:
        cur.execute(f"SELECT COUNT(*) FROM {tabla};")
        n = cur.fetchone()[0]
        volumen_total += n
        indicador = ""
        if "sentencias" in tabla and n < 1000:
            indicador = f" {ROJO}(poco volumen){RESET}"
        elif "sentencias" in tabla and n >= 10000:
            indicador = f" {VERDE}(suficiente para DL){RESET}"
        print(f"  {label:<35} {n:>12,}{indicador}")
    except:
        print(f"  {label:<35} {'N/A':>12}")

# Score volumen: sentencias es lo crítico
if total_sent >= 20000:
    score_volumen = 100
elif total_sent >= 10000:
    score_volumen = 85
elif total_sent >= 5000:
    score_volumen = 70
elif total_sent >= 1000:
    score_volumen = 50
else:
    score_volumen = 20
scores["Volumen"] = score_volumen

frag_per_sent = total_frag / total_sent if total_sent else 0
print(f"\n  Fragmentos por sentencia (avg): {frag_per_sent:.1f}")
print(f"  Score volumen: {score_label(score_volumen)}")

# ══════════════════════════════════════════════════════════════
# 6. CALIDAD DE EMBEDDINGS (distribución de similitud)
# ══════════════════════════════════════════════════════════════
separador("6. CALIDAD DE EMBEDDINGS (muestra)")

# Muestra pequeña fija para no bloquear con CROSS JOIN sobre 779K vectores
cur.execute("""
    WITH muestra AS (
        SELECT id, tipo_fragmento, embedding_fragmento
        FROM fragmentos_texto
        WHERE embedding_fragmento IS NOT NULL
        ORDER BY RANDOM()
        LIMIT 50
    )
    SELECT AVG(1 - (m.embedding_fragmento <=> n.embedding_fragmento)) AS sim_avg
    FROM muestra m
    CROSS JOIN LATERAL (
        SELECT embedding_fragmento
        FROM fragmentos_texto
        WHERE embedding_fragmento IS NOT NULL AND id != m.id
        ORDER BY m.embedding_fragmento <=> embedding_fragmento
        LIMIT 5
    ) n;
""")
sim_avg = cur.fetchone()[0]
sim_avg = float(sim_avg) if sim_avg else 0

# Similitud intra-clase usando solo la muestra anterior
cur.execute("""
    WITH muestra AS (
        SELECT id, tipo_fragmento, embedding_fragmento
        FROM fragmentos_texto
        WHERE embedding_fragmento IS NOT NULL AND tipo_fragmento IS NOT NULL
        ORDER BY RANDOM()
        LIMIT 50
    )
    SELECT m.tipo_fragmento,
           AVG(1 - (m.embedding_fragmento <=> n.embedding_fragmento)) as sim_intra
    FROM muestra m
    CROSS JOIN LATERAL (
        SELECT embedding_fragmento
        FROM fragmentos_texto
        WHERE embedding_fragmento IS NOT NULL AND id != m.id
        ORDER BY m.embedding_fragmento <=> embedding_fragmento
        LIMIT 5
    ) n
    GROUP BY m.tipo_fragmento;
""")
sim_intra = cur.fetchall()

print(f"\n  Similitud semántica promedio (top-5 vecinos, muestra 200):  {sim_avg:.4f}  ({sim_avg*100:.1f}%)")

if sim_avg > 0.85:
    print(f"  {VERDE}► Los embeddings están muy bien agrupados (alta cohesión interna).{RESET}")
elif sim_avg > 0.65:
    print(f"  {VERDE}► Buena separación semántica — ideal para clustering y retrieval.{RESET}")
elif sim_avg > 0.45:
    print(f"  {AMARILLO}► Similitud moderada — embeddings usables pero con algo de ruido.{RESET}")
else:
    print(f"  {ROJO}► Baja cohesión — revisar calidad de los textos fuente.{RESET}")

if sim_intra:
    print(f"\n  Similitud intra-clase por tipo de fragmento:")
    for tipo, sim in sim_intra:
        print(f"    {str(tipo):<25}  {sim:.4f}  ({sim*100:.1f}%)")

score_emb = min(100, sim_avg * 100 * 1.2)  # escalar a 100
scores["Calidad Embeddings"] = score_emb

# ══════════════════════════════════════════════════════════════
# 7. DUPLICADOS Y RUIDO
# ══════════════════════════════════════════════════════════════
separador("7. DUPLICADOS Y RUIDO")

cur.execute("""
    SELECT COUNT(*) FROM (
        SELECT numero_sentencia, COUNT(*) as n
        FROM sentencias
        WHERE numero_sentencia IS NOT NULL
        GROUP BY numero_sentencia
        HAVING COUNT(*) > 1
    ) dups;
""")
sent_dup = cur.fetchone()[0]
pct_dup = sent_dup / total_sent * 100 if total_sent else 0

cur.execute("""
    SELECT COUNT(*) FROM (
        SELECT contenido, COUNT(*) as n
        FROM fragmentos_texto
        WHERE contenido IS NOT NULL
        GROUP BY contenido
        HAVING COUNT(*) > 1
    ) dups;
""")
frag_dup = cur.fetchone()[0]
pct_frag_dup = frag_dup / total_frag * 100 if total_frag else 0

print(f"\n  Sentencias con número duplicado : {sent_dup:,}  ({pct_dup:.2f}%)")
print(f"  Fragmentos con contenido duplicado: {frag_dup:,}  ({pct_frag_dup:.2f}%)")

score_dup = 100 - (pct_dup * 5) - (pct_frag_dup * 2)
score_dup = max(0, min(100, score_dup))
scores["Limpieza (sin duplicados)"] = score_dup
print(f"\n  Score limpieza: {score_label(score_dup)}")

# ══════════════════════════════════════════════════════════════
# 8. RESUMEN FINAL — SCORE GLOBAL ML
# ══════════════════════════════════════════════════════════════
separador("RESUMEN FINAL — APTITUD PARA MACHINE LEARNING")

pesos = {
    "Cobertura Vectorial":       0.25,
    "Calidad Textual":           0.20,
    "Balance de Clases":         0.15,
    "Completitud Metadatos":     0.15,
    "Volumen":                   0.10,
    "Calidad Embeddings":        0.10,
    "Limpieza (sin duplicados)": 0.05,
}

score_global = sum(scores.get(k, 0) * v for k, v in pesos.items())

print(f"\n  {'Dimensión':<32} {'Score':>8}  {'Peso':>6}  {'Ponderado':>10}")
print(f"  {'─'*62}")
for dim, peso in pesos.items():
    s = scores.get(dim, 0)
    pond = s * peso
    indicador = "✅" if s >= 75 else ("⚠️" if s >= 50 else "❌")
    print(f"  {indicador} {dim:<30} {s:>7.1f}  {peso*100:>5.0f}%  {pond:>9.1f}")

print(f"\n  {'─'*62}")
print(f"  {BOLD}  SCORE GLOBAL ML:  {score_global:.1f} / 100{RESET}  →  {score_label(score_global)}")
print(f"  {'─'*62}")

if score_global >= 80:
    print(f"""
  {VERDE}{BOLD}► APTO PARA ML DE PRODUCCIÓN{RESET}
  La base de datos tiene suficiente volumen, cobertura vectorial
  y calidad textual para entrenar o fine-tunear modelos. Se
  puede proceder con clasificación, clustering o RAG avanzado.
""")
elif score_global >= 60:
    print(f"""
  {AMARILLO}{BOLD}► APTO CON MEJORAS MENORES{RESET}
  La base funciona para ML pero hay áreas que mejorar (ver
  dimensiones en amarillo/rojo). Se recomienda completar los
  metadatos faltantes antes del entrenamiento final.
""")
else:
    print(f"""
  {ROJO}{BOLD}► NO RECOMENDADO AÚN — REQUIERE TRABAJO{RESET}
  Hay brechas significativas de datos. Completar vectorización,
  limpiar duplicados y añadir etiquetas antes de proceder con ML.
""")

# Recomendaciones concretas
print(f"  {BOLD}Recomendaciones específicas:{RESET}")
if scores.get("Cobertura Vectorial", 0) < 95:
    faltantes = total_sent - sent_ok
    print(f"  • Completar vectorización: faltan {faltantes:,} sentencias.")
if scores.get("Balance de Clases", 0) < 60:
    print(f"  • Desbalance de clases detectado — usar class_weight o oversampling.")
if scores.get("Completitud Metadatos", 0) < 70:
    print(f"  • Metadatos incompletos — rellenar magistrado_id, tribunal_id, tipo_proceso_id.")
if pct_frag_dup > 5:
    print(f"  • Alto porcentaje de fragmentos duplicados ({pct_frag_dup:.1f}%) — deduplicar antes de entrenar.")
if frag_per_sent < 5:
    print(f"  • Pocos fragmentos por sentencia ({frag_per_sent:.1f}) — considerar re-fragmentar.")

print(f"\n{'='*60}\n")

cur.close()
conn.close()
