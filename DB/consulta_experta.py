import sys, os, pickle, time, re
import torch
from transformers import AutoTokenizer, AutoModel

print("--- INICIANDO SISTEMA LEGAL (MODO ESTABLE) ---")

# Configuracion
DB_PARAMS = {"dbname":"legal_ia","user":"root","password":"rootpassword","host":"localhost","port":"5432"}
MODELO_ID = "BAAI/bge-m3" 
CLASIFICADOR_PATH = os.path.join(os.path.dirname(__file__), "modelos_ml", "clasificador_fallos.pkl")

BOLD = ""
X = ""

def cargar_recursos():
    print("Cargando modelo BGE-M3 desde Transformers...")
    tokenizer = AutoTokenizer.from_pretrained(MODELO_ID)
    model = AutoModel.from_pretrained(MODELO_ID)
    
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.to(device)
    model.eval()
    print(f"Modelo cargado en {device.upper()}.")
    
    brain = None
    if os.path.exists(CLASIFICADOR_PATH):
        try:
            with open(CLASIFICADOR_PATH, 'rb') as f:
                brain = pickle.load(f)
                print("Clasificador experto cargado.")
        except:
            print("Aviso: No se pudo cargar el clasificador.")
    
    return tokenizer, model, brain, device

def obtener_embedding(texto, tokenizer, model, device):
    inputs = tokenizer(texto, return_tensors="pt", padding=True, truncation=True, max_length=512).to(device)
    with torch.no_grad():
        outputs = model(**inputs)
        embeddings = outputs.last_hidden_state[:, 0, :]
        embeddings = torch.nn.functional.normalize(embeddings, p=2, dim=1)
    return embeddings[0].cpu().numpy()

def buscar_y_responder(query_text, tokenizer, model, brain, device):
    import psycopg2
    t0 = time.time()
    
    # 1. Generar Vector
    query_vector = obtener_embedding(query_text, tokenizer, model, device)
    
    # 2. Conectar y buscar
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    
    cur.execute("""
        WITH resultados AS (
            SELECT 
                s.id as sentencia_id,
                s.numero_sentencia, 
                s.fallo_macro, 
                ft.contenido, 
                1 - (ft.embedding_fragmento <=> %s::vector) AS similitud,
                t.nombre as tribunal
            FROM fragmentos_texto ft
            JOIN sentencias s ON s.id = ft.sentencia_id
            LEFT JOIN tribunales t ON t.id = s.tribunal_id
            ORDER BY ft.embedding_fragmento <=> %s::vector
            LIMIT 3
        )
        SELECT 
            r.*,
            (SELECT string_agg(DISTINCT l.nombre_corto || ' (Art. ' || al.articulo_numero || ')', ', ')
             FROM sentencias_articulos_ley sal
             JOIN articulos_ley al ON al.id = sal.articulo_ley_id
             JOIN leyes_versiones lv ON lv.id = al.ley_version_id
             JOIN leyes l ON l.id = lv.ley_id
             WHERE sal.sentencia_id = r.sentencia_id) as base_legal
        FROM resultados r;
    """, (query_vector.tolist(), query_vector.tolist()))
    
    results = cur.fetchall()
    
    print("\n" + "="*50)
    print(f"CONSULTA: {query_text}")
    print("="*50)
    print(f"Busqueda completada en {time.time()-t0:.2f}s\n")
    
    for i, res in enumerate(results):
        s_id, num, fallo, texto, sim, trib, base_legal = res
        
        # Fallback Regex si no hay base legal
        if not base_legal:
            patron = r"(?i)art\w*\.?\s*(\d+)\s*(?:del|de la)?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)"
            matches = re.findall(patron, texto)
            if matches:
                base_legal = ", ".join([f"Art. {m[0]} {m[1]}" for m in matches])

        print(f"RESULTADO #{i+1} [Similitud: {sim*100:.1f}%]")
        print(f"Sentencia: {num or 'S/N'} | Tribunal: {trib or 'Desconocido'}")
        print(f"Fallo: {fallo or 'PENDIENTE'}")
        print(f"Base Legal: {base_legal or 'No se detectaron citas explicitas'}")
        
        # Buscar contenido de la ley
        if base_legal and 'Art.' in base_legal:
            print("\n--- CONTENIDO DE LA NORMA CITADA ---")
            m = re.search(r"Art\.?\s*(\d+)", base_legal)
            if m:
                art_num = m.group(1)
                cur_norm = conn.cursor()
                cur_norm.execute("""
                    SELECT al.texto_oficial, l.nombre_corto
                    FROM articulos_ley al
                    JOIN leyes_versiones lv ON lv.id = al.ley_version_id
                    JOIN leyes l ON l.id = lv.ley_id
                    WHERE al.articulo_numero = %s
                    LIMIT 1;
                """, (art_num,))
                norma = cur_norm.fetchone()
                if norma:
                    print(f"[{norma[1]}, Art. {art_num}]:")
                    print(f"   {norma[0][:500]}...")
                cur_norm.close()

        print("-" * 30)
        print(f"TEXTO: {texto[:500]}...")
        print("="*50 + "\n")

    cur.close()
    conn.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", type=str, required=True)
    args = parser.parse_args()
    
    try:
        tokenizer, model, brain, device = cargar_recursos()
        buscar_y_responder(args.query, tokenizer, model, brain, device)
    except Exception as e:
        print(f"Error critico: {e}")
