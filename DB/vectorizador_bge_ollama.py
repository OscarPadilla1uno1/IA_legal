import sys
sys.stdout.reconfigure(line_buffering=True)
import time
import requests
import psycopg2
from psycopg2.extras import execute_values

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432"
}

OLLAMA_URL = "http://localhost:11434/api/embed"
OLLAMA_MODEL = "bge-m3"
BATCH_SIZE = 128

def get_unvectorized_chunks(cursor):
    cursor.execute(f"SELECT id, contenido FROM fragmentos_texto WHERE embedding_fragmento IS NULL LIMIT {BATCH_SIZE};")
    return cursor.fetchall()

def update_embeddings(cursor, list_ids, list_embeddings):
    if not list_ids: return
    query = """
        UPDATE fragmentos_texto AS f
        SET embedding_fragmento = CAST(v.embed AS vector(1024))
        FROM (VALUES %s) AS v(id, embed)
        WHERE f.id = CAST(v.id AS UUID);
    """
    values = []
    for uuid_val, emb in zip(list_ids, list_embeddings):
        vector_str = "[" + ",".join(str(x) for x in emb) + "]"
        values.append((uuid_val, vector_str))
    execute_values(cursor, query, values)

def get_ollama_embeddings(texts):
    payload = {
        "model": OLLAMA_MODEL,
        "input": texts
    }
    try:
        response = requests.post(OLLAMA_URL, json=payload)
        if response.status_code == 200:
            return response.json().get("embeddings", [])
        return None
    except Exception as e:
        return None

def main():
    print("Iniciando inyector vectorial usando Ollama (Con tolerancia a Fallas)...")
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
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
        if not rows: break
            
        ids = [row[0] for row in rows]
        texts = [row[1] for row in rows]
        
        t0 = time.time()
        embeddings = get_ollama_embeddings(texts)
        
        if not embeddings or len(embeddings) != len(texts):
            print(f"\nAdvertencia: Falló el bloque de {len(texts)}. Ejecutando modo rescate (1 a 1)...")
            valid_ids = []
            valid_embs = []
            for i, chunk in enumerate(texts):
                emb = get_ollama_embeddings([chunk])
                if emb:
                    valid_ids.append(ids[i])
                    valid_embs.append(emb[0])
                else:
                    print(f"Fragmento TÓXICO evadido y eliminado. ID: {ids[i]}")
                    print(f"Info de texto: {chunk[:50]}...")
                    cursor.execute("DELETE FROM fragmentos_texto WHERE id = %s;", (ids[i],))
                    conn.commit()
            
            ids = valid_ids
            embeddings = valid_embs
            
        t_model = time.time() - t0
        
        t0 = time.time()
        if ids:
            update_embeddings(cursor, ids, embeddings)
            conn.commit()
        t_db = time.time() - t0
        
        processed += len(rows)
        left = total_left - processed
        
        print(f"[{processed}/{total_left}] Ollama Inyectados {len(ids)} vectores | GPU: {t_model:.2f}s | SQL: {t_db:.2f}s | Restan: {left}")

    print("\n¡Vectorización de la Base de Datos finalizada con éxito!")
    print(f"Tiempo Total: {(time.time() - start_time) / 60:.2f} minutos.")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
