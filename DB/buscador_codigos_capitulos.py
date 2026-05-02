import psycopg2
import torch
from sentence_transformers import SentenceTransformer

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432"
}

def connect_db():
    return psycopg2.connect(**DB_PARAMS)

class BuscadorMacroscopico:
    def __init__(self):
        print("Cargando modelo BGE-M3 (Modo Macroscópico)...")
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = SentenceTransformer('BAAI/bge-m3', device=self.device)
        self.conn = connect_db()
        
    def buscar(self, query, top_k=2):
        print(f"\n[BÚSQUEDA MACROSCÓPICA - CAPÍTULOS] Query: '{query}'")
        emb = self.model.encode(query, normalize_embeddings=True)
        vec_str = "[" + ",".join(str(x) for x in emb.tolist()) + "]"
        
        cursor = self.conn.cursor()
        sql = """
        SELECT 
            c.nombre_oficial,
            cap.capitulo_etiqueta,
            cap.texto_aglomerado,
            1 - (cap.embedding <=> %s::vector) as similitud
        FROM capitulos_codigo cap
        JOIN codigos_honduras c ON cap.codigo_id = c.id
        ORDER BY cap.embedding <=> %s::vector
        LIMIT %s;
        """
        
        cursor.execute(sql, (vec_str, vec_str, top_k))
        resultados = cursor.fetchall()
        cursor.close()
        
        for i, (codigo, cap, texto, sim) in enumerate(resultados, 1):
            print(f"\n--- [COMPENDIO {i}] Similitud General: {sim:.4f} ---")
            print(f"Código: {codigo} | {cap}")
            snippet = texto[:300].strip().replace('\n', ' ')
            print(f"Resumen del Capítulo: {snippet}...")
            
    def cerrar(self):
        self.conn.close()

if __name__ == "__main__":
    buscador = BuscadorMacroscopico()
    buscador.buscar("penas de prisión y multas")
    buscador.cerrar()
