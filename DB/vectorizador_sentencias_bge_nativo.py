"""
Vectorizador BGE-M3 para tabla `sentencias`.
Actualiza la columna `embedding` (VECTOR 1024) a partir de `texto_integro`.

Configuración rápida (variables de entorno):
  GPU_BATCH=32 FETCH_SIZE=256 MAX_BATCHES=1 python vectorizador_sentencias_bge_nativo.py
"""

import os
import sys
import time
import traceback

import psycopg2
import torch
from psycopg2.extras import execute_batch
from sentence_transformers import SentenceTransformer

from modelos_locales import resolver_modelo

# Flush UTF-8 para logs legibles en Windows
sys.stdout.reconfigure(encoding="utf-8")
try:
    sys.stdout = open(sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1)
except OSError:
    pass

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432",
}

# Sentencias tienen texto_integro MUCHO más largo que fragmentos.
# Bajamos batch sizes para no reventar VRAM.
GPU_BATCH  = int(os.getenv("GPU_BATCH",  "16"))
FETCH_SIZE = int(os.getenv("FETCH_SIZE", "128"))
MAX_BATCHES = int(os.getenv("MAX_BATCHES", "0"))

# Truncar texto_integro a N caracteres antes de encodear
# (evita OOM con textos de >50k chars). 0 = sin límite.
MAX_CHARS = int(os.getenv("MAX_CHARS", "8000"))


def encode_safe(model, textos, ids, device, gpu_batch):
    """
    Intenta encodear una lista de textos.
    En caso de OOM, cae a modo rescate 1-a-1 y luego a cuchilla (mitad de texto).
    Devuelve lista de (id, vector_str) para los que se pudo.
    """
    resultados = []

    try:
        embeddings = model.encode(
            textos,
            batch_size=gpu_batch,
            normalize_embeddings=True,
            show_progress_bar=False,
        )
        for fid, emb in zip(ids, embeddings):
            vec = "[" + ",".join(str(x) for x in emb.tolist()) + "]"
            resultados.append((fid, vec))
        return resultados

    except Exception as batch_error:
        print(f"  [OOM en lote] Activando rescate 1-a-1. Error: {batch_error}")
        if device == "cuda":
            torch.cuda.empty_cache()

    for fid, texto in zip(ids, textos):
        try:
            emb = model.encode([texto], normalize_embeddings=True)
            vec = "[" + ",".join(str(x) for x in emb[0].tolist()) + "]"
            resultados.append((fid, vec))
        except Exception:
            print(f"  [OOM 1-a-1] Activando cuchilla para id={fid}")
            if device == "cuda":
                torch.cuda.empty_cache()
            # Cuchilla: solo primera mitad del texto
            mitad = texto[: len(texto) // 2]
            try:
                emb = model.encode([mitad], normalize_embeddings=True)
                vec = "[" + ",".join(str(x) for x in emb[0].tolist()) + "]"
                resultados.append((fid, vec))
                print(f"  -> Cuchilla OK para id={fid}")
            except Exception as fatal:
                print(f"  -> Cuchilla fallida para id={fid}, se omite. Error: {fatal}")
                if device == "cuda":
                    torch.cuda.empty_cache()

    return resultados


def main():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    modelo_fuente = resolver_modelo("BAAI/bge-m3")

    print("=" * 60)
    print("  VECTORIZADOR BGE-M3 — tabla: sentencias")
    print("=" * 60)
    print(f"Motor: PyTorch {torch.__version__} | Dispositivo: {device.upper()}")
    print(
        f"Config: GPU_BATCH={GPU_BATCH} | FETCH_SIZE={FETCH_SIZE} | "
        f"MAX_BATCHES={MAX_BATCHES or 'sin límite'} | MAX_CHARS={MAX_CHARS or 'sin límite'}"
    )

    if device == "cuda":
        print(f"GPU: {torch.cuda.get_device_name(0)}")
        vram = torch.cuda.get_device_properties(0).total_memory / 1024**3
        print(f"VRAM Total: {vram:.1f} GB")

    print(f"\nCargando BAAI/bge-m3 desde: {modelo_fuente}")
    model = SentenceTransformer(
        modelo_fuente,
        device=device,
        local_files_only=os.path.isdir(modelo_fuente),
    )
    print("Modelo cargado.")

    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM sentencias WHERE embedding IS NULL;")
    total_pendientes = cursor.fetchone()[0]

    if total_pendientes == 0:
        print("\nTodas las sentencias ya están vectorizadas. No hay nada que hacer.")
        cursor.close()
        conn.close()
        return

    print(f"\nSentencias pendientes: {total_pendientes}")
    print("-" * 60)

    procesados = 0
    lotes = 0
    t_global = time.time()

    while True:
        if MAX_BATCHES and lotes >= MAX_BATCHES:
            print(f"Se alcanzó MAX_BATCHES={MAX_BATCHES}. Saliendo.")
            break

        cursor.execute(
            """
            SELECT id, texto_integro
            FROM sentencias
            WHERE embedding IS NULL
            ORDER BY id
            LIMIT %s;
            """,
            (FETCH_SIZE,),
        )
        rows = cursor.fetchall()
        if not rows:
            print("No quedan sentencias pendientes.")
            break

        lotes += 1
        ids = [str(row[0]) for row in rows]
        textos_raw = [row[1] or "" for row in rows]

        # Truncar si hay textos muy largos para cuidar VRAM
        if MAX_CHARS > 0:
            textos = [t[:MAX_CHARS] for t in textos_raw]
        else:
            textos = textos_raw

        print(f"Lote #{lotes}: {len(ids)} sentencias — encode iniciando...")

        t_gpu = time.time()
        resultados = encode_safe(model, textos, ids, device, GPU_BATCH)
        gpu_time = time.time() - t_gpu

        if not resultados:
            print(f"  Lote #{lotes} sin resultados, continuando...")
            procesados += len(ids)
            continue

        t_sql = time.time()
        execute_batch(
            cursor,
            """
            UPDATE sentencias
            SET embedding = %s::vector
            WHERE id = %s::uuid;
            """,
            [(vec, fid) for fid, vec in resultados],
            page_size=64,
        )
        conn.commit()
        sql_time = time.time() - t_sql

        procesados += len(ids)
        restantes = max(0, total_pendientes - procesados)
        elapsed = time.time() - t_global
        rate = procesados / elapsed if elapsed > 0 else 1
        eta_min = (restantes / rate) / 60 if rate > 0 else 0

        print(
            f"[{procesados}/{total_pendientes}] "
            f"GPU: {gpu_time:.2f}s | SQL: {sql_time:.2f}s | "
            f"OK: {len(resultados)}/{len(ids)} | "
            f"Restan: {restantes} | ETA: {eta_min:.1f} min"
        )

    total_min = (time.time() - t_global) / 60
    print("\nVectorización de sentencias finalizada.")
    print(f"Tiempo total: {total_min:.2f} minutos | Motor: PyTorch + {device.upper()}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as fatal:
        print(f"\nFallo fatal: {fatal}")
        print(traceback.format_exc())
        raise
