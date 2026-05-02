"""
Compara respuestas baseline vs mejoradas consultando la DB.

Baseline:
- ranking puramente semantico por pgvector

Mejorado:
- mismo pool semantico inicial
- reranking hibrido con:
  - similitud semantica
  - overlap lexical con la consulta
  - match de fallo_macro inferido desde la consulta
  - match de materia inferida
  - match de tipo_proceso inferido

Tambien puede correr un benchmark corto con consultas de ejemplo para medir
si la respuesta sintetizada cubre mejor los terminos esperados.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
import unicodedata
from collections import Counter

import psycopg2
import torch
from sentence_transformers import SentenceTransformer

from modelos_locales import resolver_modelo

sys.stdout.reconfigure(encoding="utf-8")
os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")


DB = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432",
}

QUERY_CANDIDATOS = """
WITH top_frag AS (
    SELECT
        f.sentencia_id,
        f.contenido,
        f.tipo_fragmento,
        1 - (f.embedding_fragmento <=> %s::vector) AS similitud
    FROM fragmentos_texto f
    WHERE f.embedding_fragmento IS NOT NULL
    ORDER BY f.embedding_fragmento <=> %s::vector
    LIMIT %s
)
SELECT
    tf.similitud,
    s.id::text,
    s.numero_sentencia,
    s.fecha_resolucion,
    COALESCE(s.fallo, '') AS fallo,
    COALESCE(s.fallo_macro, '') AS fallo_macro,
    COALESCE(t.nombre, '') AS tribunal,
    COALESCE(tp.nombre, '') AS tipo_proceso,
    COALESCE(string_agg(DISTINCT mat.nombre, ' | '), '') AS materia,
    COALESCE(tf.contenido, '') AS contenido,
    COALESCE(tf.tipo_fragmento, '') AS tipo_fragmento
FROM top_frag tf
JOIN sentencias s ON s.id = tf.sentencia_id
LEFT JOIN tribunales t ON t.id = s.tribunal_id
LEFT JOIN tipos_proceso tp ON tp.id = s.tipo_proceso_id
LEFT JOIN sentencias_materias sm ON sm.sentencia_id = s.id
LEFT JOIN materias mat ON mat.id = sm.materia_id
GROUP BY
    tf.similitud,
    s.id,
    s.numero_sentencia,
    s.fecha_resolucion,
    s.fallo,
    s.fallo_macro,
    t.nombre,
    tp.nombre,
    tf.contenido,
    tf.tipo_fragmento
ORDER BY tf.similitud DESC;
"""

TOKEN_RE = re.compile(r"[a-z0-9áéíóúñ]+", flags=re.IGNORECASE)

FALLO_HINTS = {
    "CASACION": ["casacion", "casación", "recurso de casacion", "recurso de casación"],
    "NULIDAD": ["nulidad", "nulo", "nula"],
    "SOBRESEIMIENTO": ["sobreseimiento", "sobreseer", "sobresee"],
    "ADMITIDO": ["ha lugar", "con lugar", "otorgar", "otorgamiento", "procedente"],
    "DENEGADO": ["no ha lugar", "sin lugar", "inadmis", "improcedente", "rechazo"],
}

PROCESO_HINTS = {
    "Amparo": ["amparo"],
    "Casacion": ["casacion", "casación"],
    "Habeas Corpus": ["habeas corpus", "exhibicion personal", "exhibición personal"],
    "Inconstitucionalidad": ["inconstitucionalidad", "inconstitucional"],
    "Revision": ["revision", "revisión"],
    "Recurso": ["recurso"],
}

MATERIA_HINTS = {
    "Derecho Laboral": ["laboral", "trabajador", "despido", "indemnizacion", "indemnización", "salario"],
    "Derecho Penal": ["penal", "delito", "pena", "estafa", "fraude", "acusado"],
    "Derecho Civil": ["civil", "contrato", "obligacion", "obligación", "responsabilidad", "daños"],
    "Contencioso Administrativo": ["administrativo", "licitacion", "licitación", "acto administrativo", "contencioso"],
    "Derecho Constitucional": ["constitucional", "constitucion", "constitución", "garantias", "garantías", "amparo"],
    "Derechos Humanos": ["derechos humanos", "niñez", "mujer", "discapacidad", "vulnerable"],
}

BENCHMARK_QUERIES = [
    {
        "query": "despido injustificado sin indemnizacion al trabajador",
        "expected_terms": ["laboral", "despido", "indemnizacion", "trabajador"],
    },
    {
        "query": "recurso de casacion contra sentencia civil",
        "expected_terms": ["casacion", "recurso", "sentencia", "civil"],
    },
    {
        "query": "nulidad de actuaciones por violacion al debido proceso",
        "expected_terms": ["nulidad", "debido", "proceso"],
    },
    {
        "query": "sobreseimiento definitivo en proceso penal",
        "expected_terms": ["sobreseimiento", "penal"],
    },
    {
        "query": "accion de amparo por violacion de derechos constitucionales",
        "expected_terms": ["amparo", "constitucional", "derechos"],
    },
]

_MODEL: SentenceTransformer | None = None
_MODEL_SOURCE: str | None = None
_MODEL_DEVICE: str | None = None


def normalize(texto: str) -> str:
    texto = unicodedata.normalize("NFKD", texto or "")
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    return texto.lower()


def tokenize(texto: str) -> list[str]:
    return TOKEN_RE.findall(normalize(texto))


def get_model() -> SentenceTransformer:
    global _MODEL, _MODEL_DEVICE, _MODEL_SOURCE

    if _MODEL is not None:
        return _MODEL

    _MODEL_DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
    _MODEL_SOURCE = resolver_modelo("BAAI/bge-m3")
    print(f"[IA] Preparando BGE-M3 en {_MODEL_DEVICE.upper()}...")
    print(f"[IA] Ruta resuelta: {_MODEL_SOURCE}")

    if not os.path.isdir(_MODEL_SOURCE):
        raise FileNotFoundError(
            "No se encontro el snapshot local de BAAI/bge-m3. "
            "Verifica la cache de Hugging Face antes de correr este script."
        )

    t0 = time.time()
    _MODEL = SentenceTransformer(
        _MODEL_SOURCE,
        device=_MODEL_DEVICE,
        local_files_only=True,
    )
    elapsed = time.time() - t0
    print(f"[IA] Modelo listo en {elapsed:.1f}s.\n")
    return _MODEL


def vectorizar(texto: str) -> str:
    model = get_model()
    vec = model.encode(texto, normalize_embeddings=True)
    return "[" + ",".join(str(x) for x in vec.tolist()) + "]"


def infer_query_intent(query: str) -> dict[str, str | None]:
    query_norm = normalize(query)
    intent = {"fallo_macro": None, "materia": None, "tipo_proceso": None}

    for label, hints in FALLO_HINTS.items():
        if any(hint in query_norm for hint in hints):
            intent["fallo_macro"] = label
            break

    for label, hints in PROCESO_HINTS.items():
        if any(hint in query_norm for hint in hints):
            intent["tipo_proceso"] = label
            break

    for label, hints in MATERIA_HINTS.items():
        if any(hint in query_norm for hint in hints):
            intent["materia"] = label
            break

    return intent


def lexical_overlap_score(query: str, text: str) -> float:
    query_tokens = set(tokenize(query))
    text_tokens = set(tokenize(text))
    if not query_tokens or not text_tokens:
        return 0.0
    return len(query_tokens & text_tokens) / len(query_tokens)


def fetch_candidates(query: str, candidate_pool: int) -> list[dict]:
    vec = vectorizar(query)
    raw_pool = max(candidate_pool * 8, candidate_pool)
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    try:
        cur.execute(QUERY_CANDIDATOS, (vec, vec, raw_pool))
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    candidates = []
    seen_sentencias = set()
    for row in rows:
        (
            similitud,
            sentencia_id,
            numero_sentencia,
            fecha_resolucion,
            fallo,
            fallo_macro,
            tribunal,
            tipo_proceso,
            materia,
            contenido,
            tipo_fragmento,
        ) = row
        if sentencia_id in seen_sentencias:
            continue
        seen_sentencias.add(sentencia_id)
        candidates.append(
            {
                "semantic_score": float(similitud),
                "sentencia_id": sentencia_id,
                "numero_sentencia": numero_sentencia or "N/D",
                "fecha_resolucion": str(fecha_resolucion or "N/D"),
                "fallo": fallo or "",
                "fallo_macro": fallo_macro or "",
                "tribunal": tribunal or "",
                "tipo_proceso": tipo_proceso or "",
                "materia": materia or "",
                "contenido": contenido or "",
                "tipo_fragmento": tipo_fragmento or "",
            }
        )
        if len(candidates) >= candidate_pool:
            break
    return candidates


def rerank_candidates(query: str, candidates: list[dict]) -> list[dict]:
    intent = infer_query_intent(query)
    reranked = []
    for item in candidates:
        combined = " ".join(
            [
                item["fallo"],
                item["fallo_macro"],
                item["tipo_proceso"],
                item["materia"],
                item["contenido"],
            ]
        )
        overlap = lexical_overlap_score(query, combined)
        fallo_bonus = 1.0 if intent["fallo_macro"] and item["fallo_macro"] == intent["fallo_macro"] else 0.0
        proceso_bonus = (
            1.0
            if intent["tipo_proceso"] and normalize(intent["tipo_proceso"]) in normalize(item["tipo_proceso"])
            else 0.0
        )
        materia_bonus = (
            1.0
            if intent["materia"] and normalize(intent["materia"]) in normalize(item["materia"])
            else 0.0
        )

        hybrid_score = (
            item["semantic_score"] * 0.72
            + overlap * 0.15
            + fallo_bonus * 0.08
            + proceso_bonus * 0.03
            + materia_bonus * 0.02
        )

        enriched = dict(item)
        enriched["lexical_overlap"] = overlap
        enriched["fallo_bonus"] = fallo_bonus
        enriched["proceso_bonus"] = proceso_bonus
        enriched["materia_bonus"] = materia_bonus
        enriched["hybrid_score"] = hybrid_score
        reranked.append(enriched)

    reranked.sort(key=lambda row: row["hybrid_score"], reverse=True)
    return reranked


def synthesize_answer(query: str, results: list[dict], mode_name: str, top: int) -> str:
    selected = results[:top]
    if not selected:
        return f"{mode_name}: sin resultados."

    fallo_dominante = Counter(item["fallo_macro"] or "SIN_FALLO" for item in selected).most_common(1)[0][0]
    materia_dominante = Counter(item["materia"] or "SIN_MATERIA" for item in selected).most_common(1)[0][0]

    lines = [
        f"{mode_name}: para '{query}' la evidencia dominante apunta a {fallo_dominante.lower()}.",
        f"Materia mas frecuente: {materia_dominante}.",
    ]
    for idx, item in enumerate(selected, start=1):
        resumen = item["contenido"].strip().replace("\n", " ")
        resumen = resumen[:220]
        lines.append(
            f"[{idx}] Sentencia {item['numero_sentencia']} | {item['fallo_macro'] or 'N/D'} | "
            f"{item['tipo_proceso'] or 'N/D'} | sim={item['semantic_score']:.3f} | {resumen}"
        )
    return " ".join(lines)


def keyword_hit_score(text: str, expected_terms: list[str]) -> float:
    text_norm = normalize(text)
    if not expected_terms:
        return 0.0
    hits = sum(1 for term in expected_terms if normalize(term) in text_norm)
    return hits / len(expected_terms)


def top_label_match(results: list[dict], intent: dict[str, str | None]) -> dict[str, float]:
    if not results:
        return {"fallo": 0.0, "materia": 0.0, "proceso": 0.0}

    top = results[0]
    fallo = 1.0 if intent["fallo_macro"] and top["fallo_macro"] == intent["fallo_macro"] else 0.0
    materia = 1.0 if intent["materia"] and normalize(intent["materia"]) in normalize(top["materia"]) else 0.0
    proceso = 1.0 if intent["tipo_proceso"] and normalize(intent["tipo_proceso"]) in normalize(top["tipo_proceso"]) else 0.0
    return {"fallo": fallo, "materia": materia, "proceso": proceso}


def print_results(label: str, results: list[dict], top: int, improved: bool) -> None:
    print(f"\n{label}")
    print("-" * 88)
    for idx, item in enumerate(results[:top], start=1):
        score_label = "hybrid" if improved else "semantic"
        score_value = item["hybrid_score"] if improved else item["semantic_score"]
        print(
            f"[{idx}] score_{score_label}={score_value:.4f} | sem={item['semantic_score']:.4f} | "
            f"fallo={item['fallo_macro'] or 'N/D'} | materia={item['materia'] or 'N/D'} | "
            f"proceso={item['tipo_proceso'] or 'N/D'} | sentencia={item['numero_sentencia']}"
        )
        if improved:
            print(
                f"    overlap={item['lexical_overlap']:.3f} "
                f"fallo_bonus={item['fallo_bonus']:.1f} "
                f"proceso_bonus={item['proceso_bonus']:.1f} "
                f"materia_bonus={item['materia_bonus']:.1f}"
            )
        print(f"    {item['contenido'].strip().replace(chr(10), ' ')[:260]}")


def compare_single_query(query: str, top: int, candidate_pool: int) -> dict:
    t0 = time.time()
    candidates = fetch_candidates(query, candidate_pool)
    baseline = sorted(candidates, key=lambda row: row["semantic_score"], reverse=True)
    improved = rerank_candidates(query, candidates)
    elapsed_ms = (time.time() - t0) * 1000

    intent = infer_query_intent(query)
    baseline_answer = synthesize_answer(query, baseline, "Baseline", top=min(3, top))
    improved_answer = synthesize_answer(query, improved, "Mejorado", top=min(3, top))
    baseline_hits = top_label_match(baseline, intent)
    improved_hits = top_label_match(improved, intent)

    print("\n" + "=" * 92)
    print(f"Consulta: {query}")
    print(f"Tiempo total: {elapsed_ms:.1f} ms | candidate_pool={candidate_pool} | top={top}")
    print(f"Intento inferido: {intent}")
    print("=" * 92)

    print_results("Top baseline", baseline, top, improved=False)
    print("\nRespuesta baseline:")
    print(baseline_answer)

    print_results("Top mejorado", improved, top, improved=True)
    print("\nRespuesta mejorada:")
    print(improved_answer)

    print("\nMetricas de alineacion del top-1:")
    print(
        f"  Fallo   -> baseline={baseline_hits['fallo']:.0f} | mejorado={improved_hits['fallo']:.0f}\n"
        f"  Materia -> baseline={baseline_hits['materia']:.0f} | mejorado={improved_hits['materia']:.0f}\n"
        f"  Proceso -> baseline={baseline_hits['proceso']:.0f} | mejorado={improved_hits['proceso']:.0f}"
    )

    return {
        "query": query,
        "intent": intent,
        "baseline_answer": baseline_answer,
        "improved_answer": improved_answer,
        "baseline_results": baseline,
        "improved_results": improved,
        "baseline_hits": baseline_hits,
        "improved_hits": improved_hits,
    }


def run_benchmark(top: int, candidate_pool: int) -> None:
    print("\n" + "=" * 92)
    print("BENCHMARK DE RESPUESTAS")
    print("=" * 92)
    baseline_scores = []
    improved_scores = []
    baseline_label_hits = []
    improved_label_hits = []

    for case in BENCHMARK_QUERIES:
        result = compare_single_query(case["query"], top=top, candidate_pool=candidate_pool)
        baseline_score = keyword_hit_score(result["baseline_answer"], case["expected_terms"])
        improved_score = keyword_hit_score(result["improved_answer"], case["expected_terms"])
        baseline_scores.append(baseline_score)
        improved_scores.append(improved_score)

        baseline_label_hits.append(sum(result["baseline_hits"].values()) / 3.0)
        improved_label_hits.append(sum(result["improved_hits"].values()) / 3.0)

        print(
            f"\nCobertura esperada -> baseline={baseline_score:.2f} | mejorado={improved_score:.2f} "
            f"| esperados={case['expected_terms']}"
        )
        print("-" * 92)

    avg_base = sum(baseline_scores) / len(baseline_scores)
    avg_improved = sum(improved_scores) / len(improved_scores)
    avg_base_labels = sum(baseline_label_hits) / len(baseline_label_hits)
    avg_improved_labels = sum(improved_label_hits) / len(improved_label_hits)

    print("\n" + "=" * 92)
    print("RESUMEN BENCHMARK")
    print("=" * 92)
    print(f"Promedio cobertura baseline   : {avg_base:.3f}")
    print(f"Promedio cobertura mejorado   : {avg_improved:.3f}")
    print(f"Delta cobertura               : {avg_improved - avg_base:+.3f}")
    print(f"Promedio top-1 baseline       : {avg_base_labels:.3f}")
    print(f"Promedio top-1 mejorado       : {avg_improved_labels:.3f}")
    print(f"Delta top-1                   : {avg_improved_labels - avg_base_labels:+.3f}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Comparador baseline vs mejorado consultando la DB")
    parser.add_argument("--query", type=str, help="Consulta unica en lenguaje natural")
    parser.add_argument("--top", type=int, default=5, help="Cantidad de resultados a mostrar")
    parser.add_argument("--candidate-pool", type=int, default=20, help="Candidatos semanticos iniciales")
    parser.add_argument("--benchmark", action="store_true", help="Corre benchmark con consultas de ejemplo")
    args = parser.parse_args()

    if args.benchmark:
        run_benchmark(top=args.top, candidate_pool=args.candidate_pool)
        return

    if not args.query:
        parser.error("Debes usar --query o --benchmark")
    compare_single_query(args.query, top=args.top, candidate_pool=args.candidate_pool)


if __name__ == "__main__":
    main()
