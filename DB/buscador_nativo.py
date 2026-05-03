import sys
import os
import torch
from transformers import AutoTokenizer, AutoModel
import psycopg2
import time

# Forzar salida en UTF-8 para evitar caracteres raros en la terminal
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

print(f"\n{'═'*80}")
print("  MOTOR DE BÚSQUEDA NATIVO (IA LEGAL HONDURAS)")
print("  Tecnología: BGE-M3 + Transformers + PyTorch")
print(f"{'═'*80}\n")

DB = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432",
}

# ── Carga del modelo ────────────────────────────────────────────────────────

device = "cuda" if torch.cuda.is_available() else "cpu"
# Usamos el resolver_modelo para encontrar el path local si existe
from modelos_locales import resolver_modelo
modelo_id = "BAAI/bge-m3"
path_modelo = resolver_modelo(modelo_id)

print(f"[IA] Cargando modelo en {device.upper()}...")
print(f"[IA] Origen: {path_modelo}")

tokenizer = AutoTokenizer.from_pretrained(path_modelo)
model = AutoModel.from_pretrained(path_modelo).to(device)
model.eval()

print("[IA] Modelo listo.\n")

# ── Funciones Core ──────────────────────────────────────────────────────────

def vectorizar(texto: str) -> str:
    """Genera embedding denso usando el token [CLS] de BGE-M3."""
    with torch.no_grad():
        inputs = tokenizer(texto, padding=True, truncation=True, max_length=512, return_tensors='pt').to(device)
        outputs = model(**inputs)
        # BGE-M3 usa el primer token (index 0) para representación densa
        embeddings = outputs.last_hidden_state[:, 0]
        # Normalización L2 (vital para similitud de coseno con producto punto)
        embeddings = torch.nn.functional.normalize(embeddings, p=2, dim=1)
        return "[" + ",".join(str(x) for x in embeddings[0].tolist()) + "]"

def separador(titulo: str):
    print(f"\n{'─'*80}\n  {titulo}\n{'─'*80}")

def buscar(query: str, top: int = 10):
    t_inicio = time.time()
    vec_str = vectorizar(query)
    
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()

    # 1. BÚSQUEDA EN JURISPRUDENCIA (SENTENCIAS)
    query_juris = """
    WITH mejor_frag AS (
        SELECT DISTINCT ON (f.sentencia_id)
            f.sentencia_id, f.contenido, f.tipo_fragmento,
            1 - (f.embedding_fragmento <=> %s::vector) AS similitud
        FROM fragmentos_texto f
        WHERE f.embedding_fragmento IS NOT NULL
        ORDER BY f.sentencia_id, f.embedding_fragmento <=> %s::vector
    )
    SELECT mf.similitud, s.numero_sentencia, s.fecha_resolucion, s.fallo, t.nombre as tribunal, mf.contenido
    FROM mejor_frag mf
    JOIN sentencias s ON s.id = mf.sentencia_id
    LEFT JOIN tribunales t ON t.id = s.tribunal_id
    ORDER BY mf.similitud DESC LIMIT %s;
    """
    
    # 2. BÚSQUEDA EN NORMATIVA (CÓDIGOS)
    query_norma = """
    SELECT 1 - (a.embedding <=> %s::vector) AS similitud, c.nombre_oficial, a.articulo_etiqueta, a.texto_oficial
    FROM articulos_codigo a
    JOIN codigos_honduras c ON c.id = a.codigo_id
    WHERE a.embedding IS NOT NULL
    ORDER BY a.embedding <=> %s::vector LIMIT %s;
    """

    print(f"[Info] Consulta: '{query}'")
    
    # Ejecutar Jurisprudencia
    cur.execute(query_juris, (vec_str, vec_str, top))
    res_juris = cur.fetchall()
    
    # Ejecutar Normativa
    cur.execute(query_norma, (vec_str, vec_str, top))
    res_norma = cur.fetchall()

    t_total = (time.time() - t_inicio) * 1000
    print(f"[Info] Tiempo de respuesta: {t_total:.1f} ms\n")

    # PRESENTACIÓN
    separador(f"JURISPRUDENCIA / SENTENCIAS ({len(res_juris)} resultados)")
    for i, (sim, num, fecha, fallo, trib, cont) in enumerate(res_juris, 1):
        print(f"\n[{i}] Similitud: {sim*100:.1f}% | Sentencia: {num or 'N/D'}")
        print(f"    Tribunal: {trib or 'N/D'}")
        print(f"    Fallo (Resumen): {(fallo or 'N/D')[:150]}...")
        print(f"    Fragmento Clave:")
        print(f"      {cont.strip()[:1000]}...")

    separador(f"NORMATIVA / CÓDIGOS ({len(res_norma)} resultados)")
    for i, (sim, cod, art, txt) in enumerate(res_norma, 1):
        print(f"\n[{i}] Similitud: {sim*100:.1f}% | {cod[:70]}")
        print(f"    Artículo: {art}")
        print(f"    Contenido:")
        print(f"      {txt.strip()[:1000]}...")

    print(f"\n{'═'*80}\n")
    cur.close()
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        buscar(" ".join(sys.argv[1:]))
    else:
        while True:
            q = input("Consulta (o 'salir'): ").strip()
            if q.lower() in ['salir', 'exit', '']: break
            buscar(q)
