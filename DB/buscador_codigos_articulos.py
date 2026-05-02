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

class BuscadorMicroscopico:
    def __init__(self):
        print("Cargando modelo BGE-M3...")
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = SentenceTransformer('BAAI/bge-m3', device=self.device)
        self.conn = connect_db()
        
    def buscar(self, query, top_k=3):
        print(f"\n[BÚSQUEDA MICROSCÓPICA - ARTÍCULOS] Query: '{query}'")
        emb = self.model.encode(query, normalize_embeddings=True)
        vec_str = "[" + ",".join(str(x) for x in emb.tolist()) + "]"
        
        cursor = self.conn.cursor()
        sql = """
        SELECT 
            c.nombre_oficial,
            cap.capitulo_etiqueta,
            a.articulo_etiqueta,
            a.texto_oficial,
            1 - (a.embedding <=> %s::vector) as similitud
        FROM articulos_codigo a
        JOIN capitulos_codigo cap ON a.capitulo_id = cap.id
        JOIN codigos_honduras c ON a.codigo_id = c.id
        ORDER BY a.embedding <=> %s::vector
        LIMIT %s;
        """
        
        cursor.execute(sql, (vec_str, vec_str, top_k))
        resultados = cursor.fetchall()
        cursor.close()
        
        for i, (codigo, cap, art, texto, sim) in enumerate(resultados, 1):
            print(f"\n--- [RESULTADO {i}] Similitud: {sim:.4f} ---")
            print(f"Código: {codigo} | {cap} | {art}")
            print(f"Texto: {texto.strip()}")
            
    def cerrar(self):
        self.conn.close()

if __name__ == "__main__":
    buscador = BuscadorMicroscopico()
    print("--------------------------------------------------")
    buscador.buscar("delitos de corrupción, cohecho o malversación de caudales públicos por funcionarios", top_k=2)
    print("--------------------------------------------------")
    buscador.buscar("causales legales de divorcio o separación de la pareja", top_k=2)
    print("--------------------------------------------------")
    buscador.buscar("derecho de los trabajadores al descanso remunerado y el día domingo", top_k=2)
    buscador.cerrar()
