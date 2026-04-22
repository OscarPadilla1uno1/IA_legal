import os
import sys
import time
import traceback

import psycopg2
import torch
from psycopg2.extras import execute_batch
from sentence_transformers import SentenceTransformer

from modelos_locales import resolver_modelo

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

GPU_BATCH = int(os.getenv("GPU_BATCH", "128"))
FETCH_SIZE = int(os.getenv("FETCH_SIZE", "1024"))
MAX_BATCHES = int(os.getenv("MAX_BATCHES", "0"))


def main():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    modelo_fuente = resolver_modelo("BAAI/bge-m3")

    print(f"Motor: PyTorch {torch.__version__} | Dispositivo: {device.upper()}")
    print(
        f"Configuracion: GPU_BATCH={GPU_BATCH} | FETCH_SIZE={FETCH_SIZE} | "
        f"MAX_BATCHES={MAX_BATCHES or 'sin limite'}"
    )
    print(f"Cargando BAAI/bge-m3 desde: {modelo_fuente}")
    model = SentenceTransformer(
        modelo_fuente,
        device=device,
        local_files_only=os.path.isdir(modelo_fuente),
    )
    print("Modelo cargado.")

    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM norma_fragmentos WHERE embedding_fragmento IS NULL;")
    total_pendientes = cursor.fetchone()[0]
    print(f"Fragmentos normativos lentos por vectorizar: {total_pendientes}")
    if total_pendientes == 0:
        cursor.close()
        conn.close()
        return

    procesados = 0
    lotes = 0
    t_global = time.time()

    while True:
        if MAX_BATCHES and lotes >= MAX_BATCHES:
            print(f"Se alcanzo MAX_BATCHES={MAX_BATCHES}.")
            break

        cursor.execute(
            """
            SELECT id, contenido
            FROM norma_fragmentos
            WHERE embedding_fragmento IS NULL
            ORDER BY id
            LIMIT %s;
            """,
            (FETCH_SIZE,),
        )
        rows = cursor.fetchall()
        if not rows:
            break

        lotes += 1
        ids = [str(row[0]) for row in rows]
        textos = [row[1] for row in rows]

        try:
            t_gpu = time.time()
            embeddings = model.encode(
                textos,
                batch_size=GPU_BATCH,
                normalize_embeddings=True,
                show_progress_bar=False,
            )
            gpu_time = time.time() - t_gpu
        except Exception as exc:
            print(f"Error vectorizando lote #{lotes}: {exc}")
            print(traceback.format_exc())
            break

        params = []
        for fragment_id, embedding in zip(ids, embeddings):
            vec = "[" + ",".join(str(x) for x in embedding.tolist()) + "]"
            params.append((vec, fragment_id))

        t_sql = time.time()
        execute_batch(
            cursor,
            """
            UPDATE norma_fragmentos
            SET embedding_fragmento = %s::vector
            WHERE id = %s::uuid;
            """,
            params,
            page_size=256,
        )
        conn.commit()
        sql_time = time.time() - t_sql

        procesados += len(ids)
        elapsed = time.time() - t_global
        rate = procesados / elapsed if elapsed > 0 else 0
        restantes = total_pendientes - procesados
        eta_min = (restantes / rate) / 60 if rate > 0 else 0
        print(
            f"[{procesados}/{total_pendientes}] GPU: {gpu_time:.2f}s | SQL: {sql_time:.2f}s | "
            f"Restan: {restantes} | ETA: {eta_min:.1f} min"
        )

    cursor.close()
    conn.close()
    print(f"Vectorizacion normativa lenta finalizada en {(time.time() - t_global) / 60:.2f} min.")


if __name__ == "__main__":
    main()
