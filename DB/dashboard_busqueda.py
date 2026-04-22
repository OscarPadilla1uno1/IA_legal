from __future__ import annotations

import argparse
import json
import os
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
import torch
from dotenv import load_dotenv
from openai import OpenAI
from sentence_transformers import CrossEncoder, SentenceTransformer

from modelos_locales import resolver_modelo

load_dotenv()

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432",
}

HTML_PATH = Path(__file__).with_name("dashboard_busqueda.html")
EMBEDDING_MODEL_ID = "BAAI/bge-m3"
RERANKER_MODEL_ID = "BAAI/bge-reranker-v2-m3"
SYSTEM_PROMPT = (
    "Eres un asistente juridico especializado en derecho hondureno. "
    "Responde usando unicamente el contexto suministrado, que puede incluir "
    "jurisprudencia y normativa canonica. "
    "Cita siempre numero de sentencia o ley/articulo cuando afirmes algo. "
    "Si el contexto no alcanza, dilo con claridad."
)


class LegalSearchService:
    def __init__(self) -> None:
        requested_device = os.getenv("SEARCH_DEVICE", "cpu").strip().lower()
        self.device = self._resolve_device(requested_device)
        self.embedding_source = resolver_modelo(EMBEDDING_MODEL_ID)
        self.reranker_source = resolver_modelo(RERANKER_MODEL_ID)
        self.embedding_model: SentenceTransformer | None = None
        self.reranker_model: CrossEncoder | None = None
        self.openai_client: OpenAI | None = None
        self.model_lock = threading.Lock()
        self.start_time = time.time()

    @staticmethod
    def _resolve_device(requested_device: str) -> str:
        if requested_device in {"cuda", "gpu"} and torch.cuda.is_available():
            return "cuda"
        if requested_device == "auto":
            return "cuda" if torch.cuda.is_available() else "cpu"
        return "cpu"

    @staticmethod
    def _local_flag(model_source: str) -> bool:
        return os.path.isdir(model_source)

    @staticmethod
    def _corpus_mode(corpus: str | None) -> str:
        corpus = (corpus or "ambos").strip().lower()
        if corpus not in {"ambos", "jurisprudencia", "leyes"}:
            raise ValueError("El corpus debe ser 'ambos', 'jurisprudencia' o 'leyes'.")
        return corpus

    def _connect(self):
        return psycopg2.connect(**DB_PARAMS)

    def ensure_embedding_model(self) -> SentenceTransformer:
        with self.model_lock:
            if self.embedding_model is None:
                self.embedding_model = SentenceTransformer(
                    self.embedding_source,
                    device=self.device,
                    local_files_only=self._local_flag(self.embedding_source),
                )
        return self.embedding_model

    def ensure_reranker_model(self) -> CrossEncoder:
        with self.model_lock:
            if self.reranker_model is None:
                self.reranker_model = CrossEncoder(
                    self.reranker_source,
                    device=self.device,
                    local_files_only=self._local_flag(self.reranker_source),
                )
        return self.reranker_model

    def ensure_openai_client(self) -> OpenAI:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY no esta configurada.")
        if self.openai_client is None:
            self.openai_client = OpenAI(api_key=api_key)
        return self.openai_client

    @staticmethod
    def _to_vector_payload(vector: list[float]) -> str:
        return "[" + ",".join(str(value) for value in vector) + "]"

    @staticmethod
    def _serialize_date(value) -> str | None:
        return str(value) if value else None

    def health(self) -> dict:
        conn = self._connect()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM sentencias")
        sentencias = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM fragmentos_texto")
        fragmentos = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM fragmentos_texto WHERE embedding_fragmento IS NOT NULL")
        vectorizados = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM leyes")
        leyes = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM leyes WHERE tipo_norma = 'codigo'")
        codigos = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM articulos_ley")
        articulos = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM fragmentos_normativos")
        fragmentos_normativos = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM fragmentos_normativos WHERE embedding_fragmento IS NOT NULL")
        normativos_vectorizados = cur.fetchone()[0]
        cur.close()
        conn.close()

        return {
            "status": "ok",
            "device": self.device,
            "embedding_model_loaded": self.embedding_model is not None,
            "reranker_model_loaded": self.reranker_model is not None,
            "embedding_source": self.embedding_source,
            "reranker_source": self.reranker_source,
            "db": {
                "sentencias": sentencias,
                "fragmentos": fragmentos,
                "vectorizados": vectorizados,
                "leyes": leyes,
                "codigos": codigos,
                "articulos_ley": articulos,
                "fragmentos_normativos": fragmentos_normativos,
                "normativos_vectorizados": normativos_vectorizados,
            },
            "uptime_seconds": round(time.time() - self.start_time, 1),
        }

    def _semantic_jurisprudencia(self, conn, vector_fmt: str, top_k: int) -> list[dict]:
        cur = conn.cursor()
        cur.execute(
            """
            WITH mejores_por_sentencia AS (
                SELECT DISTINCT ON (f.sentencia_id)
                    f.id AS frag_id,
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
            SELECT frag_id, contenido, numero_sentencia, fecha_resolucion, tipo_fragmento, similitud
            FROM mejores_por_sentencia
            ORDER BY similitud DESC
            LIMIT %s;
            """,
            (vector_fmt, vector_fmt, top_k),
        )
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "source_type": "jurisprudencia",
                "source_id": str(row[0]),
                "referencia": row[2],
                "sentencia": row[2],
                "fecha": self._serialize_date(row[3]),
                "tipo": row[4],
                "similitud": float(row[5]),
                "contenido": row[1].strip().replace("\n", " "),
            }
            for row in rows
        ]

    def _semantic_normativa(self, conn, vector_fmt: str, top_k: int) -> list[dict]:
        cur = conn.cursor()
        cur.execute(
            """
            WITH mejores_por_articulo AS (
                SELECT DISTINCT ON (COALESCE(fn.articulo_id, fn.id))
                    fn.id AS frag_id,
                    fn.contenido,
                    l.tipo_norma,
                    l.nombre_oficial,
                    lv.version_label,
                    COALESCE(al.articulo_etiqueta, 'Artículo sin etiqueta') AS articulo_etiqueta,
                    COALESCE(lv.fecha_inicio_vigencia, l.fecha_vigencia, l.fecha_publicacion) AS fecha_referencia,
                    fn.tipo_fragmento,
                    1 - (fn.embedding_fragmento <=> %s::vector) AS similitud
                FROM fragmentos_normativos fn
                JOIN leyes l ON l.id = fn.ley_id
                JOIN leyes_versiones lv ON lv.id = fn.ley_version_id
                LEFT JOIN articulos_ley al ON al.id = fn.articulo_id
                WHERE fn.embedding_fragmento IS NOT NULL
                ORDER BY COALESCE(fn.articulo_id, fn.id), fn.embedding_fragmento <=> %s::vector
            )
            SELECT frag_id, contenido, tipo_norma, nombre_oficial, version_label, articulo_etiqueta,
                   fecha_referencia, tipo_fragmento, similitud
            FROM mejores_por_articulo
            ORDER BY similitud DESC
            LIMIT %s;
            """,
            (vector_fmt, vector_fmt, top_k),
        )
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "source_type": "ley",
                "source_id": str(row[0]),
                "referencia": f"{row[3]} · {row[5]}",
                "tipo_norma": row[2],
                "ley": row[3],
                "version": row[4],
                "articulo": row[5],
                "fecha": self._serialize_date(row[6]),
                "tipo": row[7],
                "similitud": float(row[8]),
                "contenido": row[1].strip().replace("\n", " "),
            }
            for row in rows
        ]

    def semantic_search(self, question: str, top_k: int = 5, corpus: str = "ambos") -> dict:
        question = question.strip()
        if not question:
            raise ValueError("La consulta no puede estar vacia.")
        corpus = self._corpus_mode(corpus)

        model = self.ensure_embedding_model()
        t0 = time.time()
        vector = model.encode(question, normalize_embeddings=True).tolist()
        encoding_ms = (time.time() - t0) * 1000
        vector_fmt = self._to_vector_payload(vector)

        conn = self._connect()
        t1 = time.time()
        results: list[dict] = []
        if corpus in {"ambos", "jurisprudencia"}:
            results.extend(self._semantic_jurisprudencia(conn, vector_fmt, top_k))
        if corpus in {"ambos", "leyes"}:
            results.extend(self._semantic_normativa(conn, vector_fmt, top_k))
        search_ms = (time.time() - t1) * 1000
        conn.close()

        results.sort(key=lambda item: item["similitud"], reverse=True)
        return {
            "mode": "semantic",
            "query": question,
            "corpus": corpus,
            "metrics": {
                "encoding_ms": round(encoding_ms, 1),
                "search_ms": round(search_ms, 1),
                "device": self.device,
                "corpus": corpus,
            },
            "results": results[:top_k],
        }

    def _hybrid_jurisprudencia(
        self,
        conn,
        vector_fmt: str,
        question: str,
        limit: int,
        vector_weight: float,
        bm25_weight: float,
    ) -> list[dict]:
        cur = conn.cursor()
        cur.execute(
            """
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
                    COALESCE(
                        ts_rank_cd(cv.tsv_contenido, plainto_tsquery('spanish', %(query)s)),
                        0
                    ) AS rank_bm25
                FROM candidatos_vector cv
                JOIN sentencias s ON s.id = cv.sentencia_id
            ),
            mejores_por_sentencia AS (
                SELECT DISTINCT ON (sentencia_id) *,
                    (%(w_vec)s * sim_vector + %(w_bm25)s * LEAST(rank_bm25 * 10, 1.0)) AS score_hibrido
                FROM con_bm25
                ORDER BY sentencia_id,
                         (%(w_vec)s * sim_vector + %(w_bm25)s * LEAST(rank_bm25 * 10, 1.0)) DESC
            )
            SELECT frag_id, contenido, numero_sentencia, fecha_resolucion, tipo_fragmento,
                   sim_vector, rank_bm25, score_hibrido
            FROM mejores_por_sentencia
            ORDER BY score_hibrido DESC
            LIMIT %(limit)s;
            """,
            {
                "vec": vector_fmt,
                "query": question,
                "w_vec": vector_weight,
                "w_bm25": bm25_weight,
                "limit": limit,
            },
        )
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "source_type": "jurisprudencia",
                "source_id": str(row[0]),
                "referencia": row[2],
                "sentencia": row[2],
                "fecha": self._serialize_date(row[3]),
                "tipo": row[4],
                "sim_vector": float(row[5]),
                "rank_bm25": float(row[6]),
                "score_hibrido": float(row[7]),
                "contenido": row[1].strip().replace("\n", " "),
            }
            for row in rows
        ]

    def _hybrid_normativa(
        self,
        conn,
        vector_fmt: str,
        question: str,
        limit: int,
        vector_weight: float,
        bm25_weight: float,
    ) -> list[dict]:
        cur = conn.cursor()
        cur.execute(
            """
            WITH candidatos_vector AS (
                SELECT
                    fn.id AS frag_id,
                    COALESCE(fn.articulo_id, fn.id) AS entidad_id,
                    fn.contenido,
                    fn.tipo_fragmento,
                    fn.tsv_contenido,
                    l.tipo_norma,
                    l.nombre_oficial,
                    lv.version_label,
                    COALESCE(al.articulo_etiqueta, 'Artículo sin etiqueta') AS articulo_etiqueta,
                    COALESCE(lv.fecha_inicio_vigencia, l.fecha_vigencia, l.fecha_publicacion) AS fecha_referencia,
                    1 - (fn.embedding_fragmento <=> %(vec)s::vector) AS sim_vector
                FROM fragmentos_normativos fn
                JOIN leyes l ON l.id = fn.ley_id
                JOIN leyes_versiones lv ON lv.id = fn.ley_version_id
                LEFT JOIN articulos_ley al ON al.id = fn.articulo_id
                WHERE fn.embedding_fragmento IS NOT NULL
                ORDER BY fn.embedding_fragmento <=> %(vec)s::vector
                LIMIT 100
            ),
            con_bm25 AS (
                SELECT
                    cv.*,
                    COALESCE(
                        ts_rank_cd(cv.tsv_contenido, plainto_tsquery('spanish', %(query)s)),
                        0
                    ) AS rank_bm25
                FROM candidatos_vector cv
            ),
            mejores_por_articulo AS (
                SELECT DISTINCT ON (entidad_id) *,
                    (%(w_vec)s * sim_vector + %(w_bm25)s * LEAST(rank_bm25 * 10, 1.0)) AS score_hibrido
                FROM con_bm25
                ORDER BY entidad_id,
                         (%(w_vec)s * sim_vector + %(w_bm25)s * LEAST(rank_bm25 * 10, 1.0)) DESC
            )
            SELECT frag_id, contenido, tipo_norma, nombre_oficial, version_label, articulo_etiqueta,
                   fecha_referencia, tipo_fragmento, sim_vector, rank_bm25, score_hibrido
            FROM mejores_por_articulo
            ORDER BY score_hibrido DESC
            LIMIT %(limit)s;
            """,
            {
                "vec": vector_fmt,
                "query": question,
                "w_vec": vector_weight,
                "w_bm25": bm25_weight,
                "limit": limit,
            },
        )
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "source_type": "ley",
                "source_id": str(row[0]),
                "referencia": f"{row[3]} · {row[5]}",
                "tipo_norma": row[2],
                "ley": row[3],
                "version": row[4],
                "articulo": row[5],
                "fecha": self._serialize_date(row[6]),
                "tipo": row[7],
                "sim_vector": float(row[8]),
                "rank_bm25": float(row[9]),
                "score_hibrido": float(row[10]),
                "contenido": row[1].strip().replace("\n", " "),
            }
            for row in rows
        ]

    def hybrid_search(
        self,
        question: str,
        top_k_retrieve: int = 10,
        top_k_final: int = 5,
        vector_weight: float = 0.7,
        bm25_weight: float = 0.3,
        corpus: str = "ambos",
    ) -> dict:
        question = question.strip()
        if not question:
            raise ValueError("La consulta no puede estar vacia.")
        corpus = self._corpus_mode(corpus)

        embedding_model = self.ensure_embedding_model()
        reranker_model = self.ensure_reranker_model()

        t0 = time.time()
        vector = embedding_model.encode(question, normalize_embeddings=True).tolist()
        encoding_ms = (time.time() - t0) * 1000
        vector_fmt = self._to_vector_payload(vector)

        conn = self._connect()
        t1 = time.time()
        candidates: list[dict] = []
        if corpus in {"ambos", "jurisprudencia"}:
            candidates.extend(
                self._hybrid_jurisprudencia(conn, vector_fmt, question, top_k_retrieve, vector_weight, bm25_weight)
            )
        if corpus in {"ambos", "leyes"}:
            candidates.extend(
                self._hybrid_normativa(conn, vector_fmt, question, top_k_retrieve, vector_weight, bm25_weight)
            )
        search_ms = (time.time() - t1) * 1000
        conn.close()

        if not candidates:
            return {
                "mode": "hybrid",
                "query": question,
                "corpus": corpus,
                "metrics": {
                    "encoding_ms": round(encoding_ms, 1),
                    "search_ms": round(search_ms, 1),
                    "rerank_ms": 0.0,
                    "device": self.device,
                    "corpus": corpus,
                },
                "results": [],
            }

        pairs = [(question, row["contenido"]) for row in candidates]
        t2 = time.time()
        scores = reranker_model.predict(pairs)
        rerank_ms = (time.time() - t2) * 1000

        for index, candidate in enumerate(candidates):
            candidate["score_reranker"] = float(scores[index])

        candidates.sort(key=lambda item: item["score_reranker"], reverse=True)
        return {
            "mode": "hybrid",
            "query": question,
            "corpus": corpus,
            "metrics": {
                "encoding_ms": round(encoding_ms, 1),
                "search_ms": round(search_ms, 1),
                "rerank_ms": round(rerank_ms, 1),
                "device": self.device,
                "corpus": corpus,
                "candidatos": len(candidates),
            },
            "results": candidates[:top_k_final],
        }

    @staticmethod
    def build_context(results: list[dict]) -> str:
        blocks = []
        for index, result in enumerate(results, start=1):
            if result["source_type"] == "jurisprudencia":
                header = [
                    f"[Fuente {index}]",
                    "Origen: jurisprudencia",
                    f"Sentencia: {result['sentencia']}",
                    f"Fecha: {result['fecha'] or 'S/F'}",
                    f"Tipo: {result['tipo']}",
                ]
            else:
                header = [
                    f"[Fuente {index}]",
                    "Origen: ley",
                    f"Tipo norma: {result.get('tipo_norma') or 'ley'}",
                    f"Ley: {result['ley']}",
                    f"Artículo: {result['articulo']}",
                    f"Versión: {result.get('version') or 'S/V'}",
                    f"Fecha vigencia/base: {result['fecha'] or 'S/F'}",
                    f"Tipo: {result['tipo']}",
                ]
            header.append(f"Texto: {result['contenido']}")
            blocks.append("\n".join(header))
        return "\n\n".join(blocks)

    def llm_search(
        self,
        question: str,
        top_k_retrieve: int = 10,
        top_k_final: int = 5,
        corpus: str = "ambos",
    ) -> dict:
        retrieval = self.hybrid_search(
            question=question,
            top_k_retrieve=top_k_retrieve,
            top_k_final=top_k_final,
            corpus=corpus,
        )
        if not retrieval["results"]:
            return {
                "mode": "llm",
                "query": question,
                "corpus": corpus,
                "metrics": retrieval["metrics"],
                "results": [],
                "answer": "No encontre fragmentos relevantes en la base de datos para responder.",
            }

        client = self.ensure_openai_client()
        context = self.build_context(retrieval["results"])
        user_message = f"Consulta del usuario:\n{question}\n\nContexto recuperado:\n{context}"

        t0 = time.time()
        response = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4o"),
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            temperature=0.2,
            max_tokens=1200,
        )
        llm_ms = (time.time() - t0) * 1000
        message = response.choices[0].message.content

        metrics = dict(retrieval["metrics"])
        metrics["llm_ms"] = round(llm_ms, 1)
        metrics["model"] = os.getenv("OPENAI_MODEL", "gpt-4o")

        return {
            "mode": "llm",
            "query": question,
            "corpus": corpus,
            "metrics": metrics,
            "results": retrieval["results"],
            "answer": message,
        }


class DashboardHandler(BaseHTTPRequestHandler):
    service: LegalSearchService | None = None

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path in {"/", "/index.html"}:
            self._serve_html()
            return
        if parsed.path == "/api/health":
            self._send_json(HTTPStatus.OK, self.service.health())
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "Ruta no encontrada."})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        try:
            payload = self._read_json()
            corpus = payload.get("corpus", "ambos")
            if parsed.path == "/api/semantic-search":
                response = self.service.semantic_search(
                    question=payload.get("query", ""),
                    top_k=int(payload.get("top_k", 5)),
                    corpus=corpus,
                )
                self._send_json(HTTPStatus.OK, response)
                return
            if parsed.path == "/api/hybrid-search":
                response = self.service.hybrid_search(
                    question=payload.get("query", ""),
                    top_k_retrieve=int(payload.get("top_k_retrieve", 10)),
                    top_k_final=int(payload.get("top_k_final", 5)),
                    corpus=corpus,
                )
                self._send_json(HTTPStatus.OK, response)
                return
            if parsed.path == "/api/llm-search":
                response = self.service.llm_search(
                    question=payload.get("query", ""),
                    top_k_retrieve=int(payload.get("top_k_retrieve", 10)),
                    top_k_final=int(payload.get("top_k_final", 5)),
                    corpus=corpus,
                )
                self._send_json(HTTPStatus.OK, response)
                return
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "Ruta no encontrada."})
        except ValueError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except Exception as exc:  # noqa: BLE001
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        message = format % args
        print(f"[HTTP] {self.address_string()} - {message}")

    def _serve_html(self) -> None:
        content = HTML_PATH.read_text(encoding="utf-8")
        body = content.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def _send_json(self, status: HTTPStatus, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def build_server(host: str, port: int) -> ThreadingHTTPServer:
    service = LegalSearchService()
    DashboardHandler.service = service
    return ThreadingHTTPServer((host, port), DashboardHandler)


def main() -> None:
    parser = argparse.ArgumentParser(description="Dashboard local para probar busquedas juridicas")
    parser.add_argument("--host", default="127.0.0.1", help="Host donde escuchar")
    parser.add_argument("--port", type=int, default=8765, help="Puerto del dashboard")
    args = parser.parse_args()

    server = build_server(args.host, args.port)
    print(f"Dashboard listo en http://{args.host}:{args.port}")
    print(f"Dispositivo de inferencia: {DashboardHandler.service.device}")
    print("Presiona Ctrl+C para detenerlo.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nCerrando dashboard...")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
