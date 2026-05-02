"""
ft_entrenar_v2.py
Fine-tuning con BatchHardTripletLoss para evitar colapso.
Formato: (text, label)
"""
import sys, os, json, time
sys.stdout.reconfigure(encoding="utf-8")

import torch
from sentence_transformers import SentenceTransformer, SentenceTransformerTrainer, SentenceTransformerTrainingArguments
from sentence_transformers.losses import BatchHardTripletLoss
from datasets import Dataset as HFDataset
from modelos_locales import resolver_modelo

# ── Config ───────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(__file__)
TRAIN_FILE  = os.path.join(BASE_DIR, "ft_v2_train.jsonl")
VAL_FILE    = os.path.join(BASE_DIR, "ft_v2_val.jsonl")
OUTPUT_DIR  = os.path.join(BASE_DIR, "modelos_ml", "bge_m3_legal_v2")
MODELO_BASE = resolver_modelo("BAAI/bge-m3")

BATCH_SIZE  = 16      # RTX 4060 8GB
EPOCHS      = 2        # 2 epochs son suficientes para corregir el espacio
LR          = 1e-5     # LR más bajo para estabilidad
MAX_SEQ_LEN = 512

def cargar_jsonl(path):
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            rows.append(json.loads(line.strip()))
    return rows

print("\n▶ Cargando datos V2...")
train_rows = cargar_jsonl(TRAIN_FILE)
val_rows   = cargar_jsonl(VAL_FILE)

train_ds = HFDataset.from_list(train_rows)
val_ds   = HFDataset.from_list(val_rows)

print(f"  Train: {len(train_ds):,} items")
device = "cuda" if torch.cuda.is_available() else "cpu"

# ── Cargar modelo ────────────────────────────────────────────────
print("\n▶ Cargando modelo base...")
model = SentenceTransformer(MODELO_BASE, device=device)
model.max_seq_length = MAX_SEQ_LEN

# ── Loss: BatchHardTripletLoss ──────────────────────────────────
# Esta loss agrupa por label automáticamente en el batch.
loss = BatchHardTripletLoss(model=model)

# ── Training args ────────────────────────────────────────────────
args = SentenceTransformerTrainingArguments(
    output_dir=OUTPUT_DIR,
    num_train_epochs=EPOCHS,
    per_device_train_batch_size=BATCH_SIZE,
    learning_rate=LR,
    warmup_ratio=0.1,
    fp16=True,
    gradient_checkpointing=True,
    eval_strategy="no",        # saltar eval para rapidez
    save_strategy="epoch",
    logging_steps=50,
    report_to="none",
    dataloader_num_workers=0,
    seed=42,
)

trainer = SentenceTransformerTrainer(
    model=model,
    args=args,
    train_dataset=train_ds,
    loss=loss,
)

print(f"\n🚀 Iniciando entrenamiento V2 (TripletLoss)...")
t0 = time.time()
trainer.train()
print(f"\n✅ Terminado en {(time.time()-t0)/60:.1f} min")

model.save(OUTPUT_DIR)
print(f"📦 Modelo guardado en {OUTPUT_DIR}")
