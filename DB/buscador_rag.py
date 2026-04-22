import sys
sys.stdout.reconfigure(encoding='utf-8')
import argparse
import time
import os
import torch
import psycopg2
from sentence_transformers import SentenceTransformer
from modelos_locales import resolver_modelo

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432"
}

# Cargar modelo BGE-M3 nativo directo en VRAM
device = "cuda" if torch.cuda.is_available() else "cpu"
modelo_fuente = resolver_modelo("BAAI/bge-m3")
print(f"Cargando BAAI/bge-m3 en [{device.upper()}] desde: {modelo_fuente}")
model = SentenceTransformer(
    modelo_fuente,
    device=device,
    local_files_only=os.path.isdir(modelo_fuente)
)
print("Modelo cargado exitosamente.\n")

def embeber_pregunta(pregunta):
    vector = model.encode(pregunta, normalize_embeddings=True)
    return vector.tolist()

def buscar_jurisprudencia(pregunta_str, limite=5):
    print(f"🔍 Buscando jurisprudencia para: '{pregunta_str}'...\n")
    
    vector = embeber_pregunta(pregunta_str)
    vector_fmt = "[" + ",".join(str(x) for x in vector) + "]"
    
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    query = """
    WITH mejores_por_sentencia AS (
        SELECT DISTINCT ON (f.sentencia_id)
            f.contenido,
            s.numero_sentencia,
            s.fecha_resolucion,
            f.tipo_fragmento,
            1 - (f.embedding_fragmento <=> %s::vector) AS similitud
        FROM fragmentos_texto f
        JOIN sentencias s ON s.id = f.sentencia_id
        WHERE f.embedding_fragmento IS NOT NULL
        ORDER BY f.sentencia_id, f.embedding_fragmento <=> %s::vector
    )
    SELECT *
    FROM mejores_por_sentencia
    ORDER BY similitud DESC
    LIMIT %s;
    """
    
    try:
        t0 = time.time()
        cursor.execute(query, (vector_fmt, vector_fmt, limite))
        resultados = cursor.fetchall()
        t_db = time.time() - t0
        
        print(f"⏱️ PostgreSQL respondió en {t_db*1000:.2f} ms | Motor: PyTorch Nativo ({device.upper()}) | Sin Ollama\n")
        
        for i, row in enumerate(resultados):
            contenido, num_sentencia, fecha, tipo, similitud = row
            similitud = float(similitud)
            
            print(f"{'='*80}")
            print(f"  EXTRACCIÓN #{i+1}")
            print(f"{'='*80}")
            print(f"  ⚖️  Sentencia: {num_sentencia}")
            print(f"  📅 Fecha: {fecha}")
            print(f"  📂 Tipo: {tipo.upper() if tipo else 'N/A'}")
            print(f"  🎯 Similitud Semántica: {similitud*100:.2f}%")
            print(f"  📖 Contexto:")
            print(f"     {contenido.strip()[:500]}")
            print()
            
    except Exception as e:
        print(f"Error consultando DB: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Buscador Legal RAG - PyTorch Nativo')
    parser.add_argument('--query', type=str, required=True, help='Pregunta en lenguaje natural')
    parser.add_argument('--top', type=int, default=5, help='Cantidad de resultados')
    args = parser.parse_args()
    
    buscar_jurisprudencia(args.query, args.top)
