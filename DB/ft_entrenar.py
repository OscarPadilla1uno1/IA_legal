"""
ft_entrenar.py
Fine-tuning de BAAI/bge-m3 con sentence-transformers 5.x Trainer API.
Loss: MultipleNegativesRankingLoss — pares (anchor, positive) por clase.
Hardware: RTX 4060 8GB, fp16, gradient_checkpointing.
"""
import sys, os, json, time
sys.stdout.reconfigure(encoding="utf-8")

import torch
from torch.utils.data import Dataset
from sentence_transformers import SentenceTransformer, SentenceTransformerTrainer, SentenceTransformerTrainingArguments
from sentence_transformers.losses import MultipleNegativesRankingLoss
from sentence_transformers.evaluation import EmbeddingSimilarityEvaluator
from datasets import Dataset as HFDataset

from modelos_locales import resolver_modelo

# ── Config ───────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(__file__)
TRAIN_FILE  = os.path.join(BASE_DIR, "ft_datos_train.jsonl")
VAL_FILE    = os.path.join(BASE_DIR, "ft_datos_val.jsonl")
OUTPUT_DIR  = os.path.join(BASE_DIR, "modelos_ml", "bge_m3_legal_ft")
MODELO_BASE = resolver_modelo("BAAI/bge-m3")

BATCH_SIZE  = 16      # fp16 en 8GB VRAM
EPOCHS      = 3
LR          = 2e-5
WARMUP_RATIO= 0.1
MAX_SEQ_LEN = 512

# ── Verificar archivos ───────────────────────────────────────────
for f in [TRAIN_FILE, VAL_FILE]:
    if not os.path.exists(f):
        print(f"ERROR: Falta {f}. Ejecuta primero ft_preparar_datos.py")
        sys.exit(1)

print("=" * 65)
print("  FINE-TUNING BGE-M3 — Base Legal Honduras")
print("=" * 65)
print(f"  Modelo base  : {MODELO_BASE}")
print(f"  Output dir   : {OUTPUT_DIR}")
print(f"  Batch size   : {BATCH_SIZE}")
print(f"  Epochs       : {EPOCHS}")
print(f"  Learning rate: {LR}")
print(f"  Max seq len  : {MAX_SEQ_LEN}")
device = "cuda" if torch.cuda.is_available() else "cpu"
if device == "cuda":
    vram = torch.cuda.get_device_properties(0).total_memory / 1024**3
    print(f"  GPU          : {torch.cuda.get_device_name(0)}  ({vram:.1f} GB VRAM)")
print("=" * 65)

# ── Cargar datos ─────────────────────────────────────────────────
def cargar_jsonl(path):
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            rows.append(json.loads(line.strip()))
    return rows

print("\n▶ Cargando datos...")
train_rows = cargar_jsonl(TRAIN_FILE)
val_rows   = cargar_jsonl(VAL_FILE)
print(f"  Train: {len(train_rows):,} pares")
print(f"  Val:   {len(val_rows):,} pares")

# Convertir a HuggingFace Dataset (formato requerido por ST 5.x Trainer)
train_ds = HFDataset.from_list([
    {"anchor": r["anchor"], "positive": r["positive"]}
    for r in train_rows
])
val_ds = HFDataset.from_list([
    {"anchor": r["anchor"], "positive": r["positive"]}
    for r in val_rows
])

# ── Cargar modelo ────────────────────────────────────────────────
print("\n▶ Cargando modelo base BGE-M3...")
t0 = time.time()
model = SentenceTransformer(
    MODELO_BASE,
    device=device,
    local_files_only=os.path.isdir(MODELO_BASE),
)
model.max_seq_length = MAX_SEQ_LEN
print(f"  Modelo cargado en {time.time()-t0:.1f}s")

# ── Loss ─────────────────────────────────────────────────────────
# MNRL: cada (anchor, positive) del batch, los otros positives son negativos
loss = MultipleNegativesRankingLoss(model)

# ── Evaluador ────────────────────────────────────────────────────
# Usamos los pares de validación para medir similitud coseno anchor-positive
evaluator = EmbeddingSimilarityEvaluator(
    sentences1=[r["anchor"]   for r in val_rows[:500]],
    sentences2=[r["positive"] for r in val_rows[:500]],
    scores=[1.0] * 500,   # todos son positivos
    name="val_legal",
    write_csv=True,
    show_progress_bar=False,
)

# ── Training args ────────────────────────────────────────────────
steps_per_epoch = len(train_rows) // BATCH_SIZE
total_steps     = steps_per_epoch * EPOCHS
warmup_steps    = int(total_steps * WARMUP_RATIO)

print(f"\n▶ Configuración de entrenamiento:")
print(f"  Steps/epoch : {steps_per_epoch:,}")
print(f"  Total steps : {total_steps:,}")
print(f"  Warmup steps: {warmup_steps:,}")

args = SentenceTransformerTrainingArguments(
    output_dir=OUTPUT_DIR,
    num_train_epochs=EPOCHS,
    per_device_train_batch_size=BATCH_SIZE,
    per_device_eval_batch_size=BATCH_SIZE,
    learning_rate=LR,
    warmup_steps=warmup_steps,
    fp16=True,                     # Ahorra ~40% VRAM
    bf16=False,                    # RTX 4060 soporta fp16 mejor
    gradient_checkpointing=True,   # Ahorra ~30% VRAM más
    eval_strategy="epoch",
    save_strategy="epoch",
    logging_steps=50,
    load_best_model_at_end=True,
    metric_for_best_model="val_legal_pearson_cosine",
    greater_is_better=True,
    report_to="none",              # Sin wandb
    dataloader_num_workers=0,      # Windows no soporta multiprocessing fork
    seed=42,
)

# ── Trainer ──────────────────────────────────────────────────────
trainer = SentenceTransformerTrainer(
    model=model,
    args=args,
    train_dataset=train_ds,
    eval_dataset=val_ds,
    loss=loss,
    evaluator=evaluator,
)

# ── Entrenar ─────────────────────────────────────────────────────
print(f"\n{'='*65}")
print(f"  INICIANDO ENTRENAMIENTO")
print(f"  Tiempo estimado: ~45-60 min en RTX 4060")
print(f"{'='*65}\n")

t_inicio = time.time()
trainer.train()
t_total = (time.time() - t_inicio) / 60

print(f"\n✅ Entrenamiento completado en {t_total:.1f} min")

# ── Guardar modelo final ─────────────────────────────────────────
print(f"\n▶ Guardando modelo fine-tuned en: {OUTPUT_DIR}")
model.save(OUTPUT_DIR)
print(f"✅ Modelo guardado.")

# ── Evaluación rápida post-training ─────────────────────────────
print(f"\n▶ Evaluación final del evaluador:")
score_final = evaluator(model)
print(f"  Pearson coseno (val): {score_final:.4f}")

print(f"""
{'='*65}
  FINE-TUNING COMPLETADO
{'='*65}
  Modelo guardado en  : {OUTPUT_DIR}
  Tiempo total        : {t_total:.1f} min

  Para usar el modelo fine-tuned:
    model = SentenceTransformer('{OUTPUT_DIR}')

  Siguiente paso:
    python DB/ft_evaluar.py   ← medir ganancia vs. baseline
{'='*65}
""")
