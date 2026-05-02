import os
import psycopg2
import torch
from psycopg2.extras import execute_batch
from sentence_transformers import SentenceTransformer

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432"
}

def vectorizar_tabla(cursor, conn, model, tabla, col_id, col_texto, col_vector):
    cursor.execute(f"SELECT COUNT(*) FROM {tabla} WHERE {col_vector} IS NULL;")
    total = cursor.fetchone()[0]
    if total == 0:
        print(f"[{tabla}] Todo está vectorizado.")
        return
        
    print(f"[{tabla}] Vectorizando {total} registros...")
    
    cursor.execute(f"SELECT {col_id}, {col_texto} FROM {tabla} WHERE {col_vector} IS NULL;")
    rows = cursor.fetchall()
    
    ids = [str(r[0]) for r in rows]
    textos = [str(r[1]) for r in rows]
    
    embeddings = model.encode(textos, batch_size=32, normalize_embeddings=True, show_progress_bar=False)
    
    params = []
    for row_id, emb in zip(ids, embeddings):
        vec = "[" + ",".join(str(x) for x in emb.tolist()) + "]"
        params.append((vec, row_id))
        
    execute_batch(
        cursor,
        f"UPDATE {tabla} SET {col_vector} = %s::vector WHERE {col_id} = %s::uuid;",
        params,
        page_size=256
    )
    conn.commit()
    print(f"[{tabla}] Vectorización completada.")

def main():
    print("Cargando modelo BGE-M3...")
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = SentenceTransformer('BAAI/bge-m3', device=device)
    
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    print("\nIniciando vectorización multinivel...")
    vectorizar_tabla(cursor, conn, model, "capitulos_codigo", "id", "texto_aglomerado", "embedding")
    vectorizar_tabla(cursor, conn, model, "articulos_codigo", "id", "texto_oficial", "embedding")
    vectorizar_tabla(cursor, conn, model, "gacetas_oficiales", "id", "texto_integro", "embedding")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
