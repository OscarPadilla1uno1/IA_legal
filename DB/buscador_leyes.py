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

class BuscadorNormativo:
    def __init__(self):
        print("Cargando modelo de embeddings BGE-M3 (CPU/GPU)...")
        # El usuario ya tiene BGE-M3 descargado en hf cache, lo usamos desde SentenceTransformer
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = SentenceTransformer('BAAI/bge-m3', device=self.device)
        self.conn = connect_db()
        
    def buscar(self, query, top_k=10):
        print(f"\nGenerando embedding para: '{query}'...")
        query_embedding = self.model.encode(query, normalize_embeddings=True)
        vec_str = "[" + ",".join(str(x) for x in query_embedding.tolist()) + "]"
        
        cursor = self.conn.cursor()
        
        sql = """
        SELECT 
            l.nombre_oficial,
            al.articulo_etiqueta,
            al.texto_oficial,
            1 - (f.embedding_fragmento <=> %s::vector) as similitud
        FROM fragmentos_normativos f
        JOIN leyes l ON f.ley_id = l.id
        JOIN articulos_ley al ON f.articulo_id = al.id
        WHERE f.tipo_fragmento = 'articulo'
        ORDER BY f.embedding_fragmento <=> %s::vector
        LIMIT %s;
        """
        
        cursor.execute(sql, (vec_str, vec_str, top_k))
        resultados = cursor.fetchall()
        cursor.close()
        
        print("\n=== RESULTADOS DE BÚSQUEDA DUAL (SEMÁNTICA) ===")
        for i, (ley, art, texto, sim) in enumerate(resultados, 1):
            print(f"\n[TOP {i}] - Similitud: {sim:.4f}")
            print(f"LEY: {ley}")
            print(f"ARTÍCULO: {art}")
            print(f"TEXTO: {texto}")
            
    def cerrar(self):
        self.conn.close()

if __name__ == "__main__":
    buscador = BuscadorNormativo()
    
    queries = [
        "¿Cuáles son los castigos o penas por corrupción o malversación de fondos públicos?",
        "¿Qué leyes protegen el medio ambiente o áreas protegidas?"
    ]
    
    for q in queries:
        buscador.buscar(q, top_k=10)
        
    buscador.cerrar()
