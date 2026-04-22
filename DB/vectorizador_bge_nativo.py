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

# Ajustables por entorno para depuraciones rápidas:
#   GPU_BATCH=16 FETCH_SIZE=32 MAX_BATCHES=1 python vectorizador_bge_nativo.py
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
    if device == "cuda":
        print(f"GPU: {torch.cuda.get_device_name(0)}")
        vram = torch.cuda.get_device_properties(0).total_memory / 1024**3
        print(f"VRAM Total: {vram:.1f} GB")

    print(f"Cargando BAAI/bge-m3 desde: {modelo_fuente}")
    model = SentenceTransformer(
        modelo_fuente,
        device=device,
        local_files_only=os.path.isdir(modelo_fuente),
    )
    print("Modelo cargado.")

    print("Conectando a PostgreSQL...")
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    print("Contando fragmentos pendientes...")
    cursor.execute("SELECT COUNT(*) FROM fragmentos_texto WHERE embedding_fragmento IS NULL;")
    total_pendientes = cursor.fetchone()[0]

    if total_pendientes == 0:
        print("No hay pendientes. Reseteando vectores para re-procesar...")
        cursor.execute("UPDATE fragmentos_texto SET embedding_fragmento = NULL;")
        conn.commit()
        cursor.execute("SELECT COUNT(*) FROM fragmentos_texto;")
        total_pendientes = cursor.fetchone()[0]

    print(f"Fragmentos por vectorizar: {total_pendientes}")

    procesados = 0
    lotes = 0
    t_global = time.time()

    while True:
        if MAX_BATCHES and lotes >= MAX_BATCHES:
            print(f"Se alcanzo MAX_BATCHES={MAX_BATCHES}. Saliendo para inspeccion.")
            break

        print(f"Solicitando lote #{lotes + 1}...")
        cursor.execute(
            """
            SELECT id, contenido
            FROM fragmentos_texto
            WHERE embedding_fragmento IS NULL
            ORDER BY id
            LIMIT %s;
            """,
            (FETCH_SIZE,),
        )
        rows = cursor.fetchall()
        if not rows:
            print("No quedan filas pendientes.")
            break

        lotes += 1
        print(f"Lote #{lotes} recibido con {len(rows)} filas.")

        ids = [str(row[0]) for row in rows]
        textos = [row[1] for row in rows]

        t_gpu = time.time()
        try:
            print(f"Iniciando encode de lote #{lotes}...")
            embeddings = model.encode(
                textos,
                batch_size=GPU_BATCH,
                normalize_embeddings=True,
                show_progress_bar=False,
            )
            print(f"Encode de lote #{lotes} completado.")
        except Exception as batch_error:
            print(f"Error en encode de lote #{lotes}: {batch_error}")
            print(traceback.format_exc())
            print("Modo rescate 1-a-1...")
            for fid, texto in zip(ids, textos):
                try:
                    emb = model.encode([texto], normalize_embeddings=True)
                    vec = "[" + ",".join(str(x) for x in emb[0].tolist()) + "]"
                    cursor.execute(
                        """
                        UPDATE fragmentos_texto
                        SET embedding_fragmento = %s::vector
                        WHERE id = %s::uuid;
                        """,
                        (vec, fid),
                    )
                    conn.commit()
                except Exception as single_error:
                    print(f"Toxico eliminado: {fid} | {texto[:60]}...")
                    print(f"Error individual: {single_error}")
                    cursor.execute("DELETE FROM fragmentos_texto WHERE id = %s::uuid;", (fid,))
                    conn.commit()
            procesados += len(ids)
            continue

        gpu_time = time.time() - t_gpu

        t_sql = time.time()
        params = []
        for fid, emb in zip(ids, embeddings):
            vec = "[" + ",".join(str(x) for x in emb.tolist()) + "]"
            params.append((vec, fid))

        print(f"Escribiendo lote #{lotes} en PostgreSQL...")
        execute_batch(
            cursor,
            """
            UPDATE fragmentos_texto
            SET embedding_fragmento = %s::vector
            WHERE id = %s::uuid;
            """,
            params,
            page_size=256,
        )
        conn.commit()
        print(f"Commit de lote #{lotes} completado.")
        sql_time = time.time() - t_sql

        procesados += len(ids)
        restantes = total_pendientes - procesados

        elapsed = time.time() - t_global
        rate = procesados / elapsed if elapsed > 0 else 0
        eta_min = (restantes / rate) / 60 if rate > 0 else 0

        print(
            f"[{procesados}/{total_pendientes}] GPU: {gpu_time:.2f}s | SQL: {sql_time:.2f}s | "
            f"Restan: {restantes} | ETA: {eta_min:.1f} min"
        )

    total_min = (time.time() - t_global) / 60
    print("Vectorizacion nativa finalizada.")
    print(f"Tiempo total: {total_min:.2f} minutos.")
    print(f"Motor: PyTorch nativo + {device.upper()}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as fatal_error:
        print(f"Fallo fatal en vectorizador: {fatal_error}")
        print(traceback.format_exc())
        raise
