"""
Test de Retrieval sin LLM
Ejecuta solo la búsqueda híbrida (vectorial + BM25) + re-ranking con cross-encoder.
Muestra los Top N fragmentos recuperados con todos sus scores.
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import os
import time
import torch
import psycopg2
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer, CrossEncoder
from modelos_locales import resolver_modelo

load_dotenv()

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432"
}

TOP_K_RETRIEVE = 20
TOP_K_FINAL = 5
BM25_WEIGHT = 0.3
VECTOR_WEIGHT = 0.7

# ─── Cargar modelos ──────────────────────────────────────────────
device = "cuda" if torch.cuda.is_available() else "cpu"
embedding_source = resolver_modelo("BAAI/bge-m3")
reranker_source = resolver_modelo("BAAI/bge-reranker-v2-m3")

print(f"[1/2] Cargando BGE-M3 (bi-encoder) en [{device.upper()}] desde: {embedding_source}")
modelo_embeddings = SentenceTransformer(
    embedding_source,
    device=device,
    local_files_only=os.path.isdir(embedding_source)
)

print(f"[2/2] Cargando BGE-Reranker-v2-M3 (cross-encoder) en [{device.upper()}] desde: {reranker_source}")
modelo_reranker = CrossEncoder(
    reranker_source,
    device=device,
    local_files_only=os.path.isdir(reranker_source)
)

print("Modelos cargados.\n")


def buscar_y_reranquear(pregunta):
    """Ejecuta búsqueda híbrida + re-ranking y retorna resultados."""

    # Vectorizar pregunta
    t_enc = time.time()
    vector = modelo_embeddings.encode(pregunta, normalize_embeddings=True).tolist()
    vector_fmt = "[" + ",".join(str(x) for x in vector) + "]"
    t_enc = time.time() - t_enc

    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    query = """
    WITH candidatos_vector AS (
        SELECT 
            f.id AS frag_id,
            f.sentencia_id,
            f.contenido,
            f.tipo_fragmento,
            f.tsv_contenido,
            1 - (f.embedding_fragmento <=> %(vec)s::vector) AS sim_vector
        FROM fragmentos_texto f
        WHERE f.embedding_fragmento IS NOT NULL
        ORDER BY f.embedding_fragmento <=> %(vec)s::vector
        LIMIT 100
    ),
    con_bm25 AS (
        SELECT 
            cv.*,
            s.numero_sentencia,
            s.fecha_resolucion,
            COALESCE(ts_rank_cd(cv.tsv_contenido, plainto_tsquery('spanish', %(query)s)), 0) AS rank_bm25
        FROM candidatos_vector cv
        JOIN sentencias s ON s.id = cv.sentencia_id
    ),
    mejores_por_sentencia AS (
        SELECT DISTINCT ON (sentencia_id) *,
            (%(w_vec)s * sim_vector + %(w_bm25)s * LEAST(rank_bm25 * 10, 1.0)) AS score_hibrido
        FROM con_bm25
        ORDER BY sentencia_id, (%(w_vec)s * sim_vector + %(w_bm25)s * LEAST(rank_bm25 * 10, 1.0)) DESC
    )
    SELECT frag_id, contenido, numero_sentencia, fecha_resolucion, tipo_fragmento,
           sim_vector, rank_bm25, score_hibrido
    FROM mejores_por_sentencia
    ORDER BY score_hibrido DESC
    LIMIT %(limit)s;
    """

    t_db = time.time()
    cursor.execute(query, {
        'vec': vector_fmt,
        'query': pregunta,
        'w_vec': VECTOR_WEIGHT,
        'w_bm25': BM25_WEIGHT,
        'limit': TOP_K_RETRIEVE
    })
    candidatos = cursor.fetchall()
    t_db = time.time() - t_db

    cursor.close()
    conn.close()

    print(f"⏱️  Encoding pregunta: {t_enc*1000:.1f} ms")
    print(f"⏱️  Búsqueda híbrida (vectorial+BM25): {t_db*1000:.1f} ms | {len(candidatos)} candidatos")

    if not candidatos:
        print("❌ No se encontraron resultados.")
        return

    # Re-ranking
    t_rr = time.time()
    pares = [(pregunta, c[1]) for c in candidatos]
    scores = modelo_reranker.predict(pares)
    t_rr = time.time() - t_rr

    reranked = []
    for i, cand in enumerate(candidatos):
        reranked.append({
            'contenido': cand[1],
            'sentencia': cand[2],
            'fecha': cand[3],
            'tipo': cand[4],
            'sim_vector': float(cand[5]),
            'rank_bm25': float(cand[6]),
            'score_hibrido': float(cand[7]),
            'score_reranker': float(scores[i])
        })

    reranked.sort(key=lambda x: x['score_reranker'], reverse=True)
    top = reranked[:TOP_K_FINAL]

    print(f"⏱️  Re-ranking (cross-encoder): {t_rr*1000:.1f} ms | Top {TOP_K_FINAL} seleccionados")
    print()

    # ─── Mostrar resultados ─────────────────────────────────────
    print("=" * 90)
    print(f"  TOP {TOP_K_FINAL} FRAGMENTOS RECUPERADOS (sin LLM)")
    print("=" * 90)

    for i, f in enumerate(top):
        fecha = f['fecha'] or 'S/F'
        texto_preview = f['contenido'].strip()
        # Mostrar hasta 500 caracteres del texto
        if len(texto_preview) > 500:
            texto_preview = texto_preview[:500] + "..."

        print(f"\n{'─' * 90}")
        print(f"  #{i+1}  📄 {f['sentencia']}")
        print(f"       Fecha: {fecha} | Tipo: {f['tipo']}")
        print(f"       🎯 Reranker: {f['score_reranker']:.4f} | Vector: {f['sim_vector']*100:.1f}% | BM25: {f['rank_bm25']:.4f} | Híbrido: {f['score_hibrido']:.4f}")
        print(f"{'─' * 90}")
        print(f"  {texto_preview}")

    print(f"\n{'=' * 90}")

    # Tabla comparativa: orden híbrido vs reranker
    print("\n📊 COMPARACIÓN: Orden Híbrido vs Reranker")
    print(f"{'Sentencia':<45} {'Híbrido':>10} {'Reranker':>10} {'Cambio':>8}")
    print("-" * 75)
    
    # Posiciones originales (por score híbrido)
    por_hibrido = sorted(reranked, key=lambda x: x['score_hibrido'], reverse=True)
    pos_hibrido = {r['sentencia']: idx+1 for idx, r in enumerate(por_hibrido)}
    
    for i, f in enumerate(top):
        pos_h = pos_hibrido.get(f['sentencia'], '?')
        cambio = pos_h - (i + 1)
        flecha = "↑" + str(abs(cambio)) if cambio > 0 else ("↓" + str(abs(cambio)) if cambio < 0 else "=")
        print(f"  {f['sentencia'][:43]:<45} {f['score_hibrido']:>9.4f} {f['score_reranker']:>9.4f} {flecha:>8}")


def main():
    print("=" * 90)
    print("  TEST DE RETRIEVAL — Sin LLM")
    print("  Solo búsqueda híbrida (Vector + BM25) + Re-ranking (Cross-Encoder)")
    print("=" * 90)

    while True:
        print()
        pregunta = input("📝 Consulta de prueba (o 'salir'): ").strip()

        if not pregunta or pregunta.lower() in ('salir', 'exit', 'q'):
            print("\n¡Hasta luego!")
            break

        print()
        buscar_y_reranquear(pregunta)


if __name__ == "__main__":
    main()
