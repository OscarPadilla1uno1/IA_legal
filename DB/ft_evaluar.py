"""
ft_evaluar.py
Compara el modelo BGE-M3 baseline vs. fine-tuned en clasificación y clustering.
Reutiliza la misma lógica de benchmark_ml.py pero sobre ambos modelos.
"""
import sys, os, pickle, time, statistics
sys.stdout.reconfigure(encoding="utf-8")

import numpy as np
import psycopg2
from collections import Counter
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score, StratifiedKFold
from sklearn.metrics import silhouette_score, adjusted_rand_score
from sklearn.cluster import MiniBatchKMeans
from sklearn.preprocessing import LabelEncoder
from sentence_transformers import SentenceTransformer
from modelos_locales import resolver_modelo
import torch

G="\033[92m"; Y="\033[93m"; R="\033[91m"; B="\033[94m"; BOLD="\033[1m"; X="\033[0m"

BASE_DIR   = os.path.dirname(__file__)
FT_DIR     = os.path.join(BASE_DIR, "modelos_ml", "bge_m3_legal_v2", "checkpoint-563")
BASE_MODEL = resolver_modelo("BAAI/bge-m3")

DB = {"dbname":"legal_ia","user":"root","password":"rootpassword","host":"localhost","port":"5432"}
MUESTRA_N  = 2000   # sentencias para comparación (rápido pero representativo)

if not os.path.isdir(FT_DIR):
    print(f"{R}ERROR: No se encontró el modelo fine-tuned en {FT_DIR}{X}")
    print("Ejecuta primero ft_entrenar.py")
    sys.exit(1)

device = "cuda" if torch.cuda.is_available() else "cpu"

print(f"\n{BOLD}{'='*65}{X}")
print(f"{BOLD}  EVALUACIÓN: BASELINE vs. FINE-TUNED{X}")
print(f"{BOLD}{'='*65}{X}")

# ── Cargar datos de evaluación desde la DB ────────────────────────
print(f"\n{B}▶ Cargando muestra de evaluación ({MUESTRA_N} sentencias)...{X}")
conn = psycopg2.connect(**DB)
cur  = conn.cursor()

cur.execute("""
    SELECT s.embedding::text, s.fallo_macro, m.nombre AS materia
    FROM sentencias s
    LEFT JOIN sentencias_materias sm ON sm.sentencia_id = s.id
    LEFT JOIN materias m             ON m.id = sm.materia_id
    WHERE s.embedding IS NOT NULL
      AND s.fallo_macro IS NOT NULL
      AND s.fallo_macro != 'DESCONOCIDO'
    ORDER BY RANDOM()
    LIMIT %s;
""", (MUESTRA_N,))
rows_db = cur.fetchall()

# También cargar textos para re-encodear con FT model
cur.execute("""
    SELECT LEFT(s.texto_integro, 512), s.fallo_macro, m.nombre
    FROM sentencias s
    LEFT JOIN sentencias_materias sm ON sm.sentencia_id = s.id
    LEFT JOIN materias m             ON m.id = sm.materia_id
    WHERE s.embedding IS NOT NULL
      AND s.fallo_macro IS NOT NULL
      AND s.fallo_macro != 'DESCONOCIDO'
    ORDER BY RANDOM()
    LIMIT %s;
""", (MUESTRA_N,))
rows_text = cur.fetchall()
cur.close(); conn.close()

# Preparar labels
y_labels   = np.array([r[1] for r in rows_db])
mat_labels = np.array([r[2] or "Sin Materia" for r in rows_db])
le  = LabelEncoder(); y_enc  = le.fit_transform(y_labels)
lem = LabelEncoder(); ym_enc = lem.fit_transform(mat_labels)

# Embeddings BASELINE (ya guardados en DB)
print(f"  Parseando embeddings baseline...")
X_base = np.array([
    [float(x) for x in r[0].strip("[]").split(",")]
    for r in rows_db
], dtype=np.float32)

# ── Cargar modelo fine-tuned y re-encodear ───────────────────────
textos_eval = [r[0] or "" for r in rows_text]

print(f"\n{B}▶ Cargando modelo fine-tuned y re-encodeando {MUESTRA_N} textos...{X}")
t0 = time.time()
model_ft = SentenceTransformer(FT_DIR, device=device)
X_ft = model_ft.encode(
    textos_eval,
    batch_size=32,
    normalize_embeddings=True,
    show_progress_bar=True,
)
print(f"  Fine-tuned encode: {time.time()-t0:.1f}s")

# ── Función de evaluación ────────────────────────────────────────
def evaluar(X, y, y_mat, nombre):
    print(f"\n  {BOLD}── {nombre} ──{X}")

    # Clasificación: CV 5-fold con LogisticRegression
    lr = LogisticRegression(max_iter=500, class_weight="balanced", C=1.0, random_state=42)
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    scores = cross_val_score(lr, X, y, cv=cv, scoring="accuracy")
    acc = scores.mean() * 100
    acc_std = scores.std() * 100

    # Clustering: K=6 con Silhouette + ARI
    km = MiniBatchKMeans(n_clusters=6, random_state=42, batch_size=512, n_init=5)
    labels = km.fit_predict(X)
    sil = silhouette_score(X[:1000], labels[:1000], metric="cosine")
    ari = adjusted_rand_score(y_mat, labels)

    c_acc = G if acc >= 85 else (Y if acc >= 75 else R)
    c_sil = G if sil >= 0.25 else (Y if sil >= 0.15 else R)
    c_ari = G if ari >= 0.4  else (Y if ari >= 0.2  else R)

    print(f"    Clasificación CV Accuracy : {c_acc}{acc:.1f}% ± {acc_std:.1f}%{X}")
    print(f"    Clustering Silhouette     : {c_sil}{sil:.4f}{X}")
    print(f"    Clustering ARI vs mat.    : {c_ari}{ari:.4f}{X}")
    return acc, sil, ari

acc_b, sil_b, ari_b = evaluar(X_base,  y_enc, ym_enc, "BASELINE (embedding guardado en DB)")
acc_f, sil_f, ari_f = evaluar(X_ft,    y_enc, ym_enc, "FINE-TUNED (nuevo modelo)")

# ── Tabla comparativa ────────────────────────────────────────────
print(f"\n{BOLD}{'='*65}{X}")
print(f"{BOLD}  COMPARATIVA FINAL{X}")
print(f"{BOLD}{'='*65}{X}")
print(f"\n  {'Métrica':<35} {'Baseline':>10}  {'Fine-tuned':>12}  {'Ganancia':>10}")
print(f"  {'─'*68}")

def fila(label, b, ft, fmt="{:.1f}%"):
    delta = ft - b
    c = G if delta > 0 else R
    print(f"  {label:<35} {fmt.format(b):>10}  {fmt.format(ft):>12}  {c}{'+' if delta>=0 else ''}{fmt.format(delta)}{X}")

fila("Clasificación — CV Accuracy",  acc_b, acc_f)
fila("Clustering — Silhouette",      sil_b*100, sil_f*100)
fila("Clustering — ARI vs materias", ari_b*100, ari_f*100)

mejora_acc = acc_f - acc_b
mejora_sil = (sil_f - sil_b) / max(abs(sil_b), 1e-6) * 100

print(f"\n  {BOLD}Mejora en accuracy   : {G if mejora_acc>0 else R}{'+' if mejora_acc>=0 else ''}{mejora_acc:.1f} puntos{X}")
print(f"  {BOLD}Mejora en silhouette : {G if mejora_sil>0 else R}{'+' if mejora_sil>=0 else ''}{mejora_sil:.1f}%{X}")

if acc_f >= 88:
    print(f"\n  {G}{BOLD}✅ OBJETIVO ALCANZADO — Accuracy ≥ 88%{X}")
    print(f"  {G}El modelo fine-tuned está listo para producción.{X}")
elif acc_f > acc_b:
    print(f"\n  {Y}{BOLD}⚠️  MEJORA LOGRADA pero no alcanza 88% aún.{X}")
    print(f"  Considera: más epochs, más datos de CASACION, o LoRA.")
else:
    print(f"\n  {R}{BOLD}❌ Sin mejora significativa — revisar hiperparámetros.{X}")

print(f"\n{'='*65}\n")
