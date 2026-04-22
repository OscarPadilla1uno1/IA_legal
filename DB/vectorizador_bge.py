import sys
import time
import psycopg2
from psycopg2.extras import execute_values
from sentence_transformers import SentenceTransformer

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432"
}

BATCH_SIZE = 512

def get_unvectorized_chunks(cursor):
    # Fetch a batch of text chunks that haven't been vectorized yet.
    cursor.execute(f"SELECT id, contenido FROM fragmentos_texto WHERE embedding_fragmento IS NULL LIMIT {BATCH_SIZE};")
    return cursor.fetchall()

def update_embeddings(cursor, list_ids, list_embeddings):
    # PostgreSQL UPDATE via execute_values para máxima eficiencia
    query = """
        UPDATE fragmentos_texto AS f
        SET embedding_fragmento = CAST(v.embed AS vector(1024))
        FROM (VALUES %s) AS v(id, embed)
        WHERE f.id = CAST(v.id AS UUID);
    """
    
    # We must format the embedding as a string array that postgres vector understands: '[0.1, 0.2, ...]'
    values = []
    for uuid_val, emb in zip(list_ids, list_embeddings):
        # Convert NumPy array to string list format Postgres expects
        vector_str = "[" + ",".join(str(x) for x in emb.tolist()) + "]"
        values.append((uuid_val, vector_str))
        
    execute_values(cursor, query, values)

def main():
    print("Iniciando motor de Inteligencia Artificial (BGE-M3) para Vectorización Judicial...")
    
    # 1. Cargar el modelo en la tarjeta de video (CUDA)
    print("Cargando modelo BAAI/bge-m3 en VRAM (RTX 4060)...")
    try:
        model = SentenceTransformer('BAAI/bge-m3', device='cuda')
        print("Modelo cargado exitosamente en GPU.")
    except Exception as e:
        print(f"La aceleración de GPU falló, usando CPU. Error: {e}")
        model = SentenceTransformer('BAAI/bge-m3', device='cpu')
    
    # 2. Conectar a PostgreSQL
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    # Check total
    cursor.execute("SELECT count(*) FROM fragmentos_texto WHERE embedding_fragmento IS NULL;")
    total_left = cursor.fetchone()[0]
    print(f"\nFragmentos pendientes por vectorizar: {total_left}")
    
    if total_left == 0:
        print("¡Nada que hacer! Todo está vectorizado.")
        return

    processed = 0
    start_time = time.time()
    
    while True:
        rows = get_unvectorized_chunks(cursor)
        if not rows:
            break
            
        ids = [row[0] for row in rows]
        texts = [row[1] for row in rows]
        
        # 3. Vectorizar
        t0 = time.time()
        embeddings = model.encode(texts, batch_size=64, show_progress_bar=False)
        t_model = time.time() - t0
        
        # 4. Actualizar DB
        t0 = time.time()
        update_embeddings(cursor, ids, embeddings)
        conn.commit()
        t_db = time.time() - t0
        
        processed += len(rows)
        left = total_left - processed
        
        print(f"[{processed}/{total_left}] Inyectados {len(rows)} vectores | Modelo: {t_model:.2f}s | SQL: {t_db:.2f}s | Restan: {left}")

    print("\n¡Vectorización de la Base de Datos finalizada con éxito!")
    print(f"Tiempo Total: {(time.time() - start_time) / 60:.2f} minutos.")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
