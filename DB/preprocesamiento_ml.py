from __future__ import annotations

import argparse
import hashlib
import html
import json
import math
import os
import re
import shutil
import sys
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values

# Evita un warning ruidoso de joblib/loky en Windows dentro del runtime sandbox.
os.environ.setdefault("LOKY_MAX_CPU_COUNT", "1")

from sklearn.cluster import DBSCAN, MiniBatchKMeans
from sklearn.decomposition import IncrementalPCA, PCA
from sklearn.utils.class_weight import compute_class_weight

from db_config import connect_db, describe_db_target

try:
    from bs4 import BeautifulSoup  # type: ignore
except ImportError:  # pragma: no cover
    BeautifulSoup = None

try:
    import spacy  # type: ignore
except ImportError:  # pragma: no cover
    spacy = None

try:
    from imblearn.over_sampling import SMOTE  # type: ignore
except ImportError:  # pragma: no cover
    SMOTE = None


CONTROL_RE = re.compile(r"[\x00-\x1F\x7F]")
SPACE_RE = re.compile(r"\s+")
TAG_RE = re.compile(r"<[^>]+>")
SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b.*?</\1>", flags=re.IGNORECASE | re.DOTALL)
TOKEN_RE = re.compile(r"[a-zA-Z0-9_]+", flags=re.UNICODE)

FALLBACK_STOPWORDS_ES = {
    "a", "al", "algo", "algunas", "algunos", "ante", "antes", "como", "con",
    "contra", "cual", "cuando", "de", "del", "desde", "donde", "dos", "el",
    "ella", "ellas", "ellos", "en", "entre", "era", "erais", "eran", "eras",
    "eres", "es", "esa", "esas", "ese", "eso", "esos", "esta", "estaba",
    "estado", "estais", "estamos", "estan", "estar", "estas", "este", "esto",
    "estos", "fue", "fueron", "ha", "habia", "han", "hasta", "hay", "la",
    "las", "le", "les", "lo", "los", "mas", "me", "mi", "mis", "mucho", "muy",
    "nada", "ni", "no", "nos", "nosotros", "o", "os", "otra", "otros", "para",
    "pero", "por", "porque", "que", "quien", "se", "ser", "si", "sin", "sobre",
    "soy", "su", "sus", "tambien", "te", "tenia", "tengo", "ti", "tu", "tus",
    "un", "una", "uno", "unos", "y", "ya",
}


@dataclass
class PipelineArtifacts:
    full_df: pd.DataFrame
    model_df: pd.DataFrame
    balanced_df: pd.DataFrame | None
    summary: dict


@dataclass
class CategorySchema:
    materias: list[str]
    tipos_fragmento: list[str]
    tipos_proceso: list[str]
    top_tribunales: list[str]


class UnionFind:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))
        self.rank = [0] * size

    def find(self, item: int) -> int:
        while self.parent[item] != item:
            self.parent[item] = self.parent[self.parent[item]]
            item = self.parent[item]
        return item

    def union(self, left: int, right: int) -> None:
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left == root_right:
            return
        if self.rank[root_left] < self.rank[root_right]:
            self.parent[root_left] = root_right
        elif self.rank[root_left] > self.rank[root_right]:
            self.parent[root_right] = root_left
        else:
            self.parent[root_right] = root_left
            self.rank[root_left] += 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Pipeline de preprocesamiento ML sin fine-tuning para fragmentos legales. "
            "Soporta modo en memoria para muestras y modo incremental para corpus completos."
        )
    )
    parser.add_argument("--max-rows", type=int, default=25000,
                        help="Numero maximo de fragmentos a procesar. Usa 0 para todo.")
    parser.add_argument("--fetch-size", type=int, default=2000,
                        help="Tamano del lote de lectura desde PostgreSQL.")
    parser.add_argument("--output-dir", default=str(Path(__file__).with_name("artifacts_ml")),
                        help="Directorio base donde se escriben los artefactos.")
    parser.add_argument("--output-prefix", default="fragmentos_ml",
                        help="Prefijo para los artefactos exportados.")
    parser.add_argument("--pipeline-mode", choices=("auto", "memory", "incremental"), default="auto",
                        help="Modo de ejecucion. 'incremental' esta pensado para el corpus completo.")
    parser.add_argument("--overwrite-output", action="store_true",
                        help="Sobrescribe el staging y los artefactos con el mismo prefijo si ya existen.")
    parser.add_argument("--cleanup-stage", action="store_true",
                        help="Elimina el staging intermedio al finalizar el modo incremental.")
    parser.add_argument("--min-chars", type=int, default=100,
                        help="Longitud minima esperada despues de limpieza.")
    parser.add_argument("--max-chars", type=int, default=512,
                        help="Longitud maxima esperada despues de limpieza.")
    parser.add_argument("--near-dup-threshold", type=float, default=0.90,
                        help="Umbral Jaccard para near-duplicates.")
    parser.add_argument("--near-dup-max-bucket", type=int, default=40,
                        help="Maximo de candidatos comparados por bucket heuristico.")
    parser.add_argument("--dbscan-eps", type=float, default=0.05,
                        help="Parametro eps para DBSCAN en modo memory.")
    parser.add_argument("--dbscan-max-rows", type=int, default=20000,
                        help="Numero maximo de filas para activar DBSCAN en modo memory.")
    parser.add_argument("--pca-components", type=int, default=64,
                        help="Numero de componentes PCA/IPCA. Usa 0 para omitir.")
    parser.add_argument("--clusters", type=int, default=32,
                        help="Numero de pseudo-etiquetas MiniBatchKMeans. Usa 0 para omitir.")
    parser.add_argument("--ipca-batch-size", type=int, default=4096,
                        help="Tamano objetivo de lote para partial_fit de IncrementalPCA.")
    parser.add_argument("--kmeans-batch-size", type=int, default=4096,
                        help="Tamano objetivo de lote para MiniBatchKMeans incremental.")
    parser.add_argument("--top-tribunales", type=int, default=20,
                        help="Numero de tribunales a conservar como categoria propia.")
    parser.add_argument("--balance", choices=("none", "random_oversample", "smote"),
                        default="none", help="Metodo de balanceo. Solo aplica en modo memory.")
    parser.add_argument("--lemmatize", action="store_true",
                        help="Intenta lematizar con spaCy si esta disponible.")
    parser.add_argument("--remove-stopwords", action="store_true",
                        help="Elimina stopwords. Usa spaCy si esta disponible.")
    parser.add_argument("--spacy-model", default="es_core_news_md",
                        help="Modelo spaCy a cargar si se usan lemas/stopwords.")
    parser.add_argument("--persist-db", action="store_true",
                        help="Persiste features en la tabla auxiliar fragmentos_features_ml.")
    parser.add_argument("--skip-embeddings", action="store_true",
                        help="Omite parsear embeddings y desactiva PCA e inferencias basadas en vectores.")
    parser.add_argument("--random-state", type=int, default=42,
                        help="Semilla para PCA, KMeans y balanceo.")
    return parser.parse_args()


def log(message: str) -> None:
    print(message, flush=True)


def load_spacy_model(model_name: str) -> tuple[object | None, list[str]]:
    notes: list[str] = []
    if spacy is None:
        notes.append("spaCy no esta instalado; se usa tokenizacion basica.")
        return None, notes

    try:
        nlp = spacy.load(model_name, disable=["ner", "parser"])
    except Exception as exc:  # pragma: no cover
        notes.append(f"No se pudo cargar spaCy ({model_name}): {exc}. Se usa tokenizacion basica.")
        return None, notes
    return nlp, notes


def strip_markup(text: str) -> str:
    text = html.unescape(text)
    if BeautifulSoup is not None:
        return BeautifulSoup(text, "html.parser").get_text(" ")
    text = SCRIPT_STYLE_RE.sub(" ", text)
    return TAG_RE.sub(" ", text)


def basic_tokenize(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_RE.findall(text)]


def normalize_text(
    text: str,
    *,
    use_spacy: object | None,
    lemmatize: bool,
    remove_stopwords: bool,
) -> tuple[str, list[str]]:
    if text is None:
        return "", []

    text = strip_markup(text)
    text = unicodedata.normalize("NFKC", text)
    text = CONTROL_RE.sub(" ", text)
    text = SPACE_RE.sub(" ", text).strip().lower()
    if not text:
        return "", []

    if use_spacy is not None and (lemmatize or remove_stopwords):
        doc = use_spacy(text)
        tokens = []
        for token in doc:
            if token.is_space or token.is_punct:
                continue
            if remove_stopwords and token.is_stop:
                continue
            value = token.lemma_.lower() if lemmatize else token.text.lower()
            if value:
                tokens.append(value)
    else:
        tokens = basic_tokenize(text)
        if remove_stopwords:
            tokens = [token for token in tokens if token not in FALLBACK_STOPWORDS_ES]

    normalized = " ".join(tokens)
    return normalized, tokens


def parse_pgvector(text: str | None) -> np.ndarray | None:
    if not text:
        return None
    stripped = text.strip().strip("[]")
    if not stripped:
        return None
    values = np.fromstring(stripped, sep=",", dtype=np.float32)
    if values.size == 0:
        return None
    return values


def token_shingles(tokens: list[str], size: int = 5) -> set[tuple[str, ...]]:
    if not tokens:
        return set()
    if len(tokens) < size:
        return {tuple(tokens)}
    return {tuple(tokens[index:index + size]) for index in range(len(tokens) - size + 1)}


def jaccard_similarity(left: set[tuple[str, ...]], right: set[tuple[str, ...]]) -> float:
    if not left and not right:
        return 1.0
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / len(union)


def make_near_duplicate_key(tokens: list[str]) -> str:
    if not tokens:
        return "empty"
    head = tokens[:12]
    tail = tokens[-12:] if len(tokens) > 12 else []
    bucket = str(int(len(tokens) / 10))
    payload = "|".join(head + ["..."] + tail + [bucket])
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def compute_class_weights(labels: Iterable[str]) -> dict[str, float]:
    values = [label for label in labels if label and label != "DESCONOCIDO"]
    if not values:
        return {}
    classes = np.array(sorted(set(values)))
    weights = compute_class_weight(class_weight="balanced", classes=classes, y=np.array(values))
    return {label: float(weight) for label, weight in zip(classes, weights, strict=True)}


def compute_class_weights_from_counts(counts: dict[str, int]) -> dict[str, float]:
    filtered = {label: count for label, count in counts.items() if label and label != "DESCONOCIDO" and count > 0}
    if not filtered:
        return {}
    total = sum(filtered.values())
    class_count = len(filtered)
    return {
        label: float(total / (class_count * count))
        for label, count in sorted(filtered.items())
    }


def query_fragments(max_rows: int) -> tuple[str, list[object]]:
    sql = """
    WITH materias_por_sentencia AS (
        SELECT
            sm.sentencia_id,
            array_agg(DISTINCT m.nombre ORDER BY m.nombre) AS materias
        FROM sentencias_materias sm
        JOIN materias m ON m.id = sm.materia_id
        GROUP BY sm.sentencia_id
    )
    SELECT
        ft.id::text,
        ft.sentencia_id::text,
        ft.tipo_fragmento,
        ft.orden,
        ft.contenido,
        ft.embedding_fragmento::text,
        s.fallo_macro,
        s.fallo,
        s.fecha_resolucion,
        s.fecha_sentencia_recurrida,
        tp.nombre,
        s.tribunal_id::text,
        COALESCE(mps.materias, ARRAY[]::text[])
    FROM fragmentos_texto ft
    JOIN sentencias s ON s.id = ft.sentencia_id
    LEFT JOIN tipos_proceso tp ON tp.id = s.tipo_proceso_id
    LEFT JOIN materias_por_sentencia mps ON mps.sentencia_id = s.id
    WHERE ft.contenido IS NOT NULL
      AND btrim(ft.contenido) <> ''
    ORDER BY ft.id
    """
    params: list[object] = []
    if max_rows > 0:
        sql += " LIMIT %s"
        params.append(max_rows)
    return sql, params


def count_candidate_rows(max_rows: int) -> int:
    conn = connect_db()
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM fragmentos_texto
            WHERE contenido IS NOT NULL
              AND btrim(contenido) <> '';
            """
        )
        total = int(cursor.fetchone()[0])
    conn.close()
    if max_rows > 0:
        return min(total, max_rows)
    return total


def choose_pipeline_mode(args: argparse.Namespace) -> str:
    if args.pipeline_mode != "auto":
        return args.pipeline_mode
    if args.max_rows == 0 or args.max_rows > 100000:
        return "incremental"
    return "memory"


def ensure_clean_target(path: Path, overwrite: bool) -> None:
    if not path.exists():
        return
    if not overwrite:
        raise RuntimeError(
            f"El destino {path} ya existe. Usa --overwrite-output o cambia --output-prefix."
        )
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()


def prepare_incremental_paths(args: argparse.Namespace) -> dict[str, Path]:
    base_dir = Path(args.output_dir)
    base_dir.mkdir(parents=True, exist_ok=True)

    stage_dir = base_dir / f"{args.output_prefix}_staging"
    stage_parts_dir = stage_dir / "parts"
    full_parts_dir = base_dir / f"{args.output_prefix}_full_parts"
    model_parts_dir = base_dir / f"{args.output_prefix}_model_parts"
    summary_path = base_dir / f"{args.output_prefix}_summary.json"
    embedding_path = stage_dir / "embeddings.dat"
    stage_info_path = stage_dir / "stage_info.json"
    duplicate_reps_path = stage_dir / "duplicate_reps.txt"

    for target in [stage_dir, full_parts_dir, model_parts_dir, summary_path]:
        ensure_clean_target(target, args.overwrite_output)

    stage_parts_dir.mkdir(parents=True, exist_ok=True)
    full_parts_dir.mkdir(parents=True, exist_ok=True)
    model_parts_dir.mkdir(parents=True, exist_ok=True)

    return {
        "base_dir": base_dir,
        "stage_dir": stage_dir,
        "stage_parts_dir": stage_parts_dir,
        "full_parts_dir": full_parts_dir,
        "model_parts_dir": model_parts_dir,
        "summary_path": summary_path,
        "embedding_path": embedding_path,
        "stage_info_path": stage_info_path,
        "duplicate_reps_path": duplicate_reps_path,
    }


def get_stage_part_paths(stage_parts_dir: Path) -> list[Path]:
    return sorted(stage_parts_dir.glob("part-*.parquet"))


def load_duplicate_representatives(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def write_stage_batch(stage_parts_dir: Path, part_index: int, records: list[dict]) -> int:
    if not records:
        return part_index
    frame = pd.DataFrame(records)
    frame.to_parquet(stage_parts_dir / f"part-{part_index:06d}.parquet", index=False)
    return part_index + 1


def open_embedding_memmap(embedding_path: Path, rows: int, dim: int, mode: str) -> np.memmap:
    return np.memmap(embedding_path, dtype=np.float32, mode=mode, shape=(rows, dim))


def flush_partial_fit_buffer(
    estimator,
    buffer_list: list[np.ndarray],
    min_batch: int,
    fitted_once: bool,
) -> tuple[bool, list[np.ndarray]]:
    if not buffer_list:
        return fitted_once, buffer_list
    rows = sum(chunk.shape[0] for chunk in buffer_list)
    if not fitted_once and rows < min_batch:
        return fitted_once, buffer_list
    if fitted_once and rows == 0:
        return fitted_once, buffer_list
    batch = np.concatenate(buffer_list, axis=0)
    estimator.partial_fit(batch)
    return True, []


def decode_materias_json(values: pd.Series) -> list[list[str]]:
    return [json.loads(value) if isinstance(value, str) and value else [] for value in values.tolist()]


def build_multi_hot_frame(
    values: list[list[str]],
    categories: list[str],
    prefix: str,
    index: pd.Index,
) -> pd.DataFrame:
    if not categories:
        return pd.DataFrame(index=index)
    mapping = {category: position for position, category in enumerate(categories)}
    encoded = np.zeros((len(values), len(categories)), dtype=np.int8)
    for row_index, labels in enumerate(values):
        for label in labels:
            position = mapping.get(label)
            if position is not None:
                encoded[row_index, position] = 1
    return pd.DataFrame(encoded, index=index, columns=[f"{prefix}__{category}" for category in categories])


def build_one_hot_frame(
    values: list[str],
    categories: list[str],
    prefix: str,
    index: pd.Index,
) -> pd.DataFrame:
    if not categories:
        return pd.DataFrame(index=index)
    mapping = {category: position for position, category in enumerate(categories)}
    encoded = np.zeros((len(values), len(categories)), dtype=np.int8)
    for row_index, value in enumerate(values):
        position = mapping.get(value)
        if position is not None:
            encoded[row_index, position] = 1
    return pd.DataFrame(encoded, index=index, columns=[f"{prefix}__{category}" for category in categories])


def ensure_feature_table(conn) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS fragmentos_features_ml (
                fragmento_id UUID PRIMARY KEY REFERENCES fragmentos_texto(id) ON DELETE CASCADE,
                sentencia_id UUID NOT NULL REFERENCES sentencias(id) ON DELETE CASCADE,
                label TEXT,
                texto_limpio TEXT NOT NULL,
                clean_hash TEXT NOT NULL,
                near_key TEXT NOT NULL,
                token_count INTEGER NOT NULL,
                char_count INTEGER NOT NULL,
                within_target_window BOOLEAN NOT NULL,
                tipo_fragmento TEXT,
                materias TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
                primary_materia TEXT,
                tipo_proceso TEXT,
                tribunal_id UUID,
                fecha_resolucion DATE,
                fecha_sentencia_recurrida DATE,
                anio INTEGER,
                mes INTEGER,
                dia_semana INTEGER,
                dias_desde_recurrida INTEGER,
                is_duplicate BOOLEAN NOT NULL DEFAULT FALSE,
                duplicate_group UUID,
                duplicate_of UUID,
                cluster_id INTEGER,
                embedding_reduced DOUBLE PRECISION[],
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_fragmentos_features_ml_sentencia_id
                ON fragmentos_features_ml (sentencia_id);
            CREATE INDEX IF NOT EXISTS idx_fragmentos_features_ml_label
                ON fragmentos_features_ml (label);
            CREATE INDEX IF NOT EXISTS idx_fragmentos_features_ml_cluster_id
                ON fragmentos_features_ml (cluster_id);
            CREATE INDEX IF NOT EXISTS idx_fragmentos_features_ml_duplicate_group
                ON fragmentos_features_ml (duplicate_group);
            """
        )
    conn.commit()


def persist_features_batch(conn, df: pd.DataFrame) -> None:
    pca_columns = [column for column in df.columns if column.startswith("pca_")]
    rows = []
    for row in df.itertuples(index=False):
        reduced = [float(getattr(row, column)) for column in pca_columns if pd.notna(getattr(row, column))]
        rows.append(
            (
                row.fragmento_id,
                row.sentencia_id,
                row.label,
                row.texto_limpio,
                row.clean_hash,
                row.near_key,
                int(row.token_count),
                int(row.char_count),
                bool(row.within_target_window),
                row.tipo_fragmento,
                list(row.materias),
                row.primary_materia,
                row.tipo_proceso,
                row.tribunal_id,
                row.fecha_resolucion,
                row.fecha_sentencia_recurrida,
                int(row.anio) if pd.notna(row.anio) else None,
                int(row.mes) if pd.notna(row.mes) else None,
                int(row.dia_semana) if pd.notna(row.dia_semana) else None,
                int(row.dias_desde_recurrida) if pd.notna(row.dias_desde_recurrida) else None,
                bool(row.is_duplicate),
                row.duplicate_group,
                row.duplicate_of,
                int(row.cluster_id) if pd.notna(row.cluster_id) else None,
                reduced or None,
            )
        )

    if not rows:
        return

    with conn.cursor() as cursor:
        execute_values(
            cursor,
            """
            INSERT INTO fragmentos_features_ml (
                fragmento_id, sentencia_id, label, texto_limpio, clean_hash, near_key,
                token_count, char_count, within_target_window, tipo_fragmento, materias,
                primary_materia, tipo_proceso, tribunal_id, fecha_resolucion,
                fecha_sentencia_recurrida, anio, mes, dia_semana, dias_desde_recurrida,
                is_duplicate, duplicate_group, duplicate_of, cluster_id, embedding_reduced
            ) VALUES %s
            ON CONFLICT (fragmento_id) DO UPDATE SET
                sentencia_id = EXCLUDED.sentencia_id,
                label = EXCLUDED.label,
                texto_limpio = EXCLUDED.texto_limpio,
                clean_hash = EXCLUDED.clean_hash,
                near_key = EXCLUDED.near_key,
                token_count = EXCLUDED.token_count,
                char_count = EXCLUDED.char_count,
                within_target_window = EXCLUDED.within_target_window,
                tipo_fragmento = EXCLUDED.tipo_fragmento,
                materias = EXCLUDED.materias,
                primary_materia = EXCLUDED.primary_materia,
                tipo_proceso = EXCLUDED.tipo_proceso,
                tribunal_id = EXCLUDED.tribunal_id,
                fecha_resolucion = EXCLUDED.fecha_resolucion,
                fecha_sentencia_recurrida = EXCLUDED.fecha_sentencia_recurrida,
                anio = EXCLUDED.anio,
                mes = EXCLUDED.mes,
                dia_semana = EXCLUDED.dia_semana,
                dias_desde_recurrida = EXCLUDED.dias_desde_recurrida,
                is_duplicate = EXCLUDED.is_duplicate,
                duplicate_group = EXCLUDED.duplicate_group,
                duplicate_of = EXCLUDED.duplicate_of,
                cluster_id = EXCLUDED.cluster_id,
                embedding_reduced = EXCLUDED.embedding_reduced,
                updated_at = CURRENT_TIMESTAMP
            """,
            rows,
            page_size=1000,
        )
    conn.commit()


def load_records(args: argparse.Namespace, nlp: object | None) -> tuple[pd.DataFrame, np.ndarray | None, dict]:
    sql, params = query_fragments(args.max_rows)
    conn = connect_db()
    cursor = conn.cursor(name="preprocesamiento_ml_cursor_memory")
    cursor.itersize = args.fetch_size
    cursor.execute(sql, params)

    records: list[dict] = []
    embeddings: list[np.ndarray] = []
    row_count = 0
    rows_with_embeddings = 0
    rows_without_embeddings = 0

    while True:
        rows = cursor.fetchmany(args.fetch_size)
        if not rows:
            break

        for row in rows:
            (
                fragmento_id,
                sentencia_id,
                tipo_fragmento,
                orden,
                contenido,
                embedding_text,
                fallo_macro,
                fallo,
                fecha_resolucion,
                fecha_sentencia_recurrida,
                tipo_proceso,
                tribunal_id,
                materias,
            ) = row

            texto_limpio, tokens = normalize_text(
                contenido,
                use_spacy=nlp,
                lemmatize=args.lemmatize,
                remove_stopwords=args.remove_stopwords,
            )
            if not texto_limpio:
                continue

            embedding = None if args.skip_embeddings else parse_pgvector(embedding_text)
            if embedding is not None:
                rows_with_embeddings += 1
            else:
                rows_without_embeddings += 1

            label = fallo_macro or fallo or "DESCONOCIDO"
            clean_hash = hashlib.sha1(texto_limpio.encode("utf-8")).hexdigest()
            char_count = len(texto_limpio)
            token_count = len(tokens)
            within_range = args.min_chars <= char_count <= args.max_chars
            shingles = token_shingles(tokens, size=5)
            near_key = make_near_duplicate_key(tokens)
            primary_materia = materias[0] if materias else "SIN_MATERIA"
            anio = fecha_resolucion.year if fecha_resolucion else None
            mes = fecha_resolucion.month if fecha_resolucion else None
            dia_semana = fecha_resolucion.weekday() if fecha_resolucion else None
            dias_desde_recurrida = None
            if fecha_resolucion and fecha_sentencia_recurrida:
                dias_desde_recurrida = (fecha_resolucion - fecha_sentencia_recurrida).days

            records.append({
                "fragmento_id": fragmento_id,
                "sentencia_id": sentencia_id,
                "tipo_fragmento": tipo_fragmento or "SIN_TIPO",
                "orden": int(orden) if orden is not None else -1,
                "label": label,
                "texto_limpio": texto_limpio,
                "clean_hash": clean_hash,
                "near_key": near_key,
                "token_count": token_count,
                "char_count": char_count,
                "within_target_window": within_range,
                "tokens": tokens,
                "shingles": shingles,
                "materias": list(materias or []),
                "primary_materia": primary_materia,
                "tipo_proceso": tipo_proceso or "SIN_TIPO_PROCESO",
                "tribunal_id": tribunal_id,
                "fecha_resolucion": fecha_resolucion,
                "fecha_sentencia_recurrida": fecha_sentencia_recurrida,
                "anio": anio,
                "mes": mes,
                "dia_semana": dia_semana,
                "dias_desde_recurrida": dias_desde_recurrida,
                "has_embedding": embedding is not None,
            })
            if embedding is not None:
                embeddings.append(embedding)
            row_count += 1

        log(f"[load] Procesados {row_count:,} fragmentos...")

    cursor.close()
    conn.close()

    frame = pd.DataFrame(records)
    embedding_matrix = None
    if not args.skip_embeddings and not frame.empty and frame["has_embedding"].any():
        available_indices = frame.index[frame["has_embedding"]].tolist()
        embedding_dim = len(embeddings[0]) if embeddings else 0
        embedding_matrix = np.zeros((len(frame), embedding_dim), dtype=np.float32)
        for position, embedding in zip(available_indices, embeddings, strict=True):
            embedding_matrix[position] = embedding

    summary = {
        "source_db": describe_db_target(),
        "rows_loaded": int(len(frame)),
        "rows_with_embeddings": int(rows_with_embeddings),
        "rows_without_embeddings": int(rows_without_embeddings),
    }
    return frame, embedding_matrix, summary


def detect_duplicates(
    df: pd.DataFrame,
    *,
    near_dup_threshold: float,
    near_dup_max_bucket: int,
    embedding_matrix: np.ndarray | None,
    dbscan_eps: float,
    dbscan_max_rows: int,
) -> tuple[pd.DataFrame, dict]:
    if df.empty:
        df["duplicate_group"] = []
        df["duplicate_of"] = []
        df["is_duplicate"] = []
        return df, {"exact_duplicate_groups": 0, "near_duplicate_groups": 0, "duplicates_flagged": 0}

    uf = UnionFind(len(df))
    exact_groups = df.groupby("clean_hash").indices
    exact_group_count = 0
    for indices in exact_groups.values():
        if len(indices) < 2:
            continue
        exact_group_count += 1
        anchor = indices[0]
        for other in indices[1:]:
            uf.union(anchor, other)

    near_buckets: dict[str, list[int]] = defaultdict(list)
    for index, key in enumerate(df["near_key"].tolist()):
        near_buckets[key].append(index)

    near_group_count = 0
    for indices in near_buckets.values():
        if len(indices) < 2:
            continue
        near_group_count += 1
        candidates = indices[:near_dup_max_bucket]
        for offset, left in enumerate(candidates):
            left_tokens = df.at[left, "token_count"]
            left_shingles = df.at[left, "shingles"]
            for right in candidates[offset + 1:]:
                if abs(left_tokens - df.at[right, "token_count"]) > 5:
                    continue
                score = jaccard_similarity(left_shingles, df.at[right, "shingles"])
                if score >= near_dup_threshold:
                    uf.union(left, right)

    dbscan_clusters = 0
    if embedding_matrix is not None and len(df) <= dbscan_max_rows and df["has_embedding"].any():
        valid_indices = df.index[df["has_embedding"]].tolist()
        vectors = embedding_matrix[valid_indices]
        if len(vectors) >= 2:
            model = DBSCAN(eps=dbscan_eps, min_samples=2, metric="cosine")
            labels = model.fit_predict(vectors)
            clusters: dict[int, list[int]] = defaultdict(list)
            for index, cluster_label in zip(valid_indices, labels, strict=True):
                if cluster_label >= 0:
                    clusters[int(cluster_label)].append(index)
            dbscan_clusters = len(clusters)
            for members in clusters.values():
                anchor = members[0]
                for other in members[1:]:
                    uf.union(anchor, other)

    groups: dict[int, list[int]] = defaultdict(list)
    for index in range(len(df)):
        groups[uf.find(index)].append(index)

    duplicate_group: list[str | None] = [None] * len(df)
    duplicate_of: list[str | None] = [None] * len(df)
    is_duplicate: list[bool] = [False] * len(df)

    for members in groups.values():
        if len(members) < 2:
            continue
        members_sorted = sorted(members)
        representative_id = df.at[members_sorted[0], "fragmento_id"]
        for member in members_sorted:
            duplicate_group[member] = representative_id
        for member in members_sorted[1:]:
            duplicate_of[member] = representative_id
            is_duplicate[member] = True

    df = df.copy()
    df["duplicate_group"] = duplicate_group
    df["duplicate_of"] = duplicate_of
    df["is_duplicate"] = is_duplicate

    return df, {
        "exact_duplicate_groups": int(exact_group_count),
        "near_duplicate_groups": int(near_group_count),
        "embedding_duplicate_clusters": int(dbscan_clusters),
        "duplicates_flagged": int(sum(is_duplicate)),
    }


def build_feature_matrix(
    df: pd.DataFrame,
    embedding_matrix: np.ndarray | None,
    *,
    pca_components: int,
    clusters: int,
    top_tribunales: int,
    random_state: int,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    work_df = df.copy()
    work_df["materia_count"] = work_df["materias"].apply(len)

    tribunal_counter = Counter(
        value for value in work_df["tribunal_id"].tolist()
        if pd.notna(value) and value
    )
    ordered_top_tribunales = [tribunal for tribunal, _ in tribunal_counter.most_common(top_tribunales)]
    top_tribunal_ids = set(ordered_top_tribunales)
    work_df["tribunal_bucket"] = work_df["tribunal_id"].apply(
        lambda value: value if value in top_tribunal_ids else "OTRO_TRIBUNAL"
    )

    materia_mlb = pd.DataFrame(index=work_df.index)
    if len(work_df):
        observed_materias = sorted({item for values in work_df["materias"] for item in values})
        if observed_materias:
            encoded = np.zeros((len(work_df), len(observed_materias)), dtype=np.int8)
            mapping = {name: pos for pos, name in enumerate(observed_materias)}
            for row_index, values in enumerate(work_df["materias"]):
                for value in values:
                    encoded[row_index, mapping[value]] = 1
            materia_mlb = pd.DataFrame(
                encoded,
                index=work_df.index,
                columns=[f"materia__{name}" for name in observed_materias],
            )

    categorical = pd.get_dummies(
        work_df[["tipo_fragmento", "tipo_proceso", "tribunal_bucket"]],
        prefix=["tipo_fragmento", "tipo_proceso", "tribunal"],
        dtype=np.int8,
    )

    numeric = work_df[
        [
            "orden",
            "token_count",
            "char_count",
            "within_target_window",
            "materia_count",
            "anio",
            "mes",
            "dia_semana",
            "dias_desde_recurrida",
        ]
    ].copy()
    numeric["within_target_window"] = numeric["within_target_window"].astype(np.int8)
    numeric = numeric.fillna(-1)

    dedup_mask = ~work_df["is_duplicate"]
    base_model_df = pd.concat(
        [
            work_df.loc[dedup_mask, ["fragmento_id", "sentencia_id", "label"]].reset_index(drop=True),
            numeric.loc[dedup_mask].reset_index(drop=True),
            categorical.loc[dedup_mask].reset_index(drop=True),
            materia_mlb.loc[dedup_mask].reset_index(drop=True),
        ],
        axis=1,
    )

    summary: dict[str, object] = {
        "top_tribunales": ordered_top_tribunales,
        "model_rows_after_dedup": int(len(base_model_df)),
        "numeric_feature_count": int(base_model_df.drop(columns=["fragmento_id", "sentencia_id", "label"]).shape[1]),
    }

    if embedding_matrix is None or not work_df["has_embedding"].any():
        work_df["cluster_id"] = -1
        return work_df, base_model_df, summary

    embedding_ready_mask = dedup_mask & work_df["has_embedding"]
    reduced_columns: list[str] = []
    cluster_series = pd.Series(-1, index=work_df.index, dtype=np.int32)

    if pca_components > 0:
        valid_count = int(embedding_ready_mask.sum())
        if valid_count >= 2:
            components = min(pca_components, embedding_matrix.shape[1], valid_count)
            pca = PCA(n_components=components, random_state=random_state)
            reduced = pca.fit_transform(embedding_matrix[embedding_ready_mask.values])
            reduced_columns = [f"pca_{index:03d}" for index in range(reduced.shape[1])]
            reduced_frame = pd.DataFrame(reduced, index=work_df.index[embedding_ready_mask], columns=reduced_columns)
            summary["pca_components"] = int(reduced.shape[1])
            summary["pca_explained_variance"] = float(pca.explained_variance_ratio_.sum())
        else:
            reduced_frame = pd.DataFrame(index=work_df.index)
            summary["pca_components"] = 0
            summary["pca_explained_variance"] = 0.0
    else:
        reduced_frame = pd.DataFrame(index=work_df.index)
        summary["pca_components"] = 0
        summary["pca_explained_variance"] = 0.0

    if clusters > 0 and int(embedding_ready_mask.sum()) >= 2:
        if reduced_columns:
            cluster_input = reduced_frame.loc[embedding_ready_mask, reduced_columns].to_numpy(dtype=np.float32)
        else:
            cluster_input = embedding_matrix[embedding_ready_mask.values]
        cluster_count = min(clusters, len(cluster_input))
        kmeans = MiniBatchKMeans(n_clusters=cluster_count, random_state=random_state, n_init=10)
        assigned = kmeans.fit_predict(cluster_input)
        cluster_series.loc[work_df.index[embedding_ready_mask]] = assigned.astype(np.int32)
        summary["cluster_count"] = int(cluster_count)
    else:
        summary["cluster_count"] = 0

    work_df["cluster_id"] = cluster_series

    if reduced_columns:
        reduced_full = pd.DataFrame(index=work_df.index, columns=reduced_columns, dtype=np.float32)
        reduced_full.loc[reduced_frame.index, reduced_columns] = reduced_frame
        work_df = pd.concat([work_df, reduced_full], axis=1)
        reduced_model = reduced_full.loc[dedup_mask].reset_index(drop=True).fillna(0)
        base_model_df = pd.concat([base_model_df, reduced_model], axis=1)

    base_model_df["cluster_id"] = work_df.loc[dedup_mask, "cluster_id"].reset_index(drop=True).astype(np.int32)
    summary["numeric_feature_count"] = int(base_model_df.drop(columns=["fragmento_id", "sentencia_id", "label"]).shape[1])
    return work_df, base_model_df, summary


def balance_model_df(
    model_df: pd.DataFrame,
    *,
    method: str,
    random_state: int,
) -> tuple[pd.DataFrame | None, dict]:
    if method == "none" or model_df.empty:
        return None, {"balance_method_applied": "none"}

    feature_cols = [col for col in model_df.columns if col not in {"fragmento_id", "sentencia_id", "label"}]
    X = model_df[feature_cols].fillna(0)
    y = model_df["label"]

    if method == "random_oversample":
        target = y.value_counts().max()
        parts = []
        for _, group in model_df.groupby("label", sort=True):
            sampled = group.sample(target, replace=len(group) < target, random_state=random_state)
            parts.append(sampled)
        balanced = pd.concat(parts, ignore_index=True).sample(frac=1, random_state=random_state).reset_index(drop=True)
        balanced["synthetic_row"] = False
        return balanced, {
            "balance_method_applied": "random_oversample",
            "balanced_rows": int(len(balanced)),
        }

    if method == "smote" and SMOTE is not None:
        min_class_size = int(y.value_counts().min())
        if min_class_size < 2:
            return None, {
                "balance_method_applied": "smote_skipped",
                "balance_note": "La clase minima tiene menos de 2 ejemplos.",
            }
        k_neighbors = max(1, min(5, min_class_size - 1))
        sampler = SMOTE(random_state=random_state, k_neighbors=k_neighbors)
        X_resampled, y_resampled = sampler.fit_resample(X, y)
        balanced = pd.DataFrame(X_resampled, columns=feature_cols)
        balanced.insert(0, "label", y_resampled)
        balanced.insert(0, "sentencia_id", pd.NA)
        balanced.insert(0, "fragmento_id", pd.NA)
        balanced["synthetic_row"] = [False] * len(model_df) + [True] * (len(balanced) - len(model_df))
        return balanced, {
            "balance_method_applied": "smote",
            "balanced_rows": int(len(balanced)),
            "smote_k_neighbors": int(k_neighbors),
        }

    note = "imblearn no esta instalado; se sugiere usar random_oversample o class_weight."
    return None, {
        "balance_method_applied": "smote_unavailable",
        "balance_note": note,
    }


def export_artifacts(artifacts: PipelineArtifacts, output_dir: Path, output_prefix: str) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    full_path = output_dir / f"{output_prefix}.parquet"
    model_path = output_dir / f"{output_prefix}_modelo.parquet"
    summary_path = output_dir / f"{output_prefix}_summary.json"

    export_full = artifacts.full_df.drop(columns=["tokens", "shingles"], errors="ignore")
    export_full.to_parquet(full_path, index=False)
    artifacts.model_df.to_parquet(model_path, index=False)

    balanced_path = None
    if artifacts.balanced_df is not None:
        balanced_path = output_dir / f"{output_prefix}_modelo_balanceado.parquet"
        artifacts.balanced_df.to_parquet(balanced_path, index=False)

    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(artifacts.summary, handle, ensure_ascii=False, indent=2, default=str)

    result = {
        "full_path": str(full_path),
        "model_path": str(model_path),
        "summary_path": str(summary_path),
    }
    if balanced_path is not None:
        result["balanced_path"] = str(balanced_path)
    return result


def run_pipeline_memory(args: argparse.Namespace) -> PipelineArtifacts:
    notes: list[str] = []
    nlp = None
    if args.lemmatize or args.remove_stopwords:
        nlp, nlp_notes = load_spacy_model(args.spacy_model)
        notes.extend(nlp_notes)

    df, embedding_matrix, load_summary = load_records(args, nlp)
    if df.empty:
        raise RuntimeError("No se encontraron fragmentos utilizables.")

    if args.skip_embeddings:
        notes.append("Embeddings omitidos por --skip-embeddings.")
        args.pca_components = 0
        args.clusters = 0
    elif args.max_rows == 0 and len(df) > 100000 and (args.pca_components > 0 or args.clusters > 0):
        raise RuntimeError(
            "Para PCA/KMeans sobre todo el corpus usa el modo incremental o desactiva "
            "esas etapas con --pca-components 0 --clusters 0."
        )

    df, duplicate_summary = detect_duplicates(
        df,
        near_dup_threshold=args.near_dup_threshold,
        near_dup_max_bucket=args.near_dup_max_bucket,
        embedding_matrix=embedding_matrix,
        dbscan_eps=args.dbscan_eps,
        dbscan_max_rows=args.dbscan_max_rows,
    )

    enriched_df, model_df, feature_summary = build_feature_matrix(
        df,
        embedding_matrix,
        pca_components=args.pca_components,
        clusters=args.clusters,
        top_tribunales=args.top_tribunales,
        random_state=args.random_state,
    )

    balanced_df, balance_summary = balance_model_df(
        model_df,
        method=args.balance,
        random_state=args.random_state,
    )

    label_counts = Counter(enriched_df["label"].tolist())
    summary = {
        **load_summary,
        **duplicate_summary,
        **feature_summary,
        **balance_summary,
        "label_distribution": dict(sorted(label_counts.items())),
        "class_weights_suggestion": compute_class_weights(model_df["label"].tolist()),
        "within_target_window_ratio": float(enriched_df["within_target_window"].mean()),
        "notes": notes,
    }

    if args.persist_db:
        conn = connect_db()
        ensure_feature_table(conn)
        persist_features_batch(conn, enriched_df)
        conn.close()
        summary["persisted_table"] = "fragmentos_features_ml"

    return PipelineArtifacts(
        full_df=enriched_df,
        model_df=model_df,
        balanced_df=balanced_df,
        summary=summary,
    )


def stage_incremental_corpus(args: argparse.Namespace, nlp: object | None, paths: dict[str, Path]) -> dict:
    sql, params = query_fragments(args.max_rows)
    candidate_rows = count_candidate_rows(args.max_rows)

    conn = connect_db()
    cursor = conn.cursor(name="preprocesamiento_ml_cursor_incremental")
    cursor.itersize = args.fetch_size
    cursor.execute(sql, params)

    stage_records: list[dict] = []
    stage_part_index = 1
    row_idx = 0

    rows_loaded = 0
    rows_with_embeddings = 0
    rows_without_embeddings = 0
    dedup_rows_with_embeddings = 0
    duplicates_flagged = 0
    within_target_window_hits = 0

    label_counter: Counter[str] = Counter()
    tribunal_counter: Counter[str] = Counter()
    materias_set: set[str] = set()
    tipos_fragmento_set: set[str] = set()
    tipos_proceso_set: set[str] = set()

    exact_representatives: dict[str, str] = {}
    exact_group_started: set[str] = set()
    near_candidates: dict[str, list[dict]] = defaultdict(list)
    near_group_started: set[str] = set()
    duplicate_representatives: set[str] = set()

    exact_duplicate_groups = 0
    near_duplicate_groups = 0
    embedding_duplicate_clusters = 0

    embedding_memmap = None
    embedding_dim = 0

    log(f"[stage] Candidatos a procesar: {candidate_rows:,}")

    while True:
        rows = cursor.fetchmany(args.fetch_size)
        if not rows:
            break

        for row in rows:
            (
                fragmento_id,
                sentencia_id,
                tipo_fragmento,
                orden,
                contenido,
                embedding_text,
                fallo_macro,
                fallo,
                fecha_resolucion,
                fecha_sentencia_recurrida,
                tipo_proceso,
                tribunal_id,
                materias,
            ) = row

            texto_limpio, tokens = normalize_text(
                contenido,
                use_spacy=nlp,
                lemmatize=args.lemmatize,
                remove_stopwords=args.remove_stopwords,
            )
            if not texto_limpio:
                continue

            embedding = None if args.skip_embeddings else parse_pgvector(embedding_text)
            if embedding is not None:
                if embedding_memmap is None:
                    embedding_dim = int(embedding.shape[0])
                    embedding_memmap = open_embedding_memmap(
                        paths["embedding_path"],
                        rows=max(candidate_rows, 1),
                        dim=embedding_dim,
                        mode="w+",
                    )
                embedding_memmap[row_idx, :] = embedding
                rows_with_embeddings += 1
            else:
                rows_without_embeddings += 1

            label = fallo_macro or fallo or "DESCONOCIDO"
            clean_hash = hashlib.sha1(texto_limpio.encode("utf-8")).hexdigest()
            char_count = len(texto_limpio)
            token_count = len(tokens)
            within_range = args.min_chars <= char_count <= args.max_chars
            shingles = token_shingles(tokens, size=5)
            near_key = make_near_duplicate_key(tokens)
            materias_list = list(materias or [])
            primary_materia = materias_list[0] if materias_list else "SIN_MATERIA"
            anio = fecha_resolucion.year if fecha_resolucion else None
            mes = fecha_resolucion.month if fecha_resolucion else None
            dia_semana = fecha_resolucion.weekday() if fecha_resolucion else None
            dias_desde_recurrida = None
            if fecha_resolucion and fecha_sentencia_recurrida:
                dias_desde_recurrida = (fecha_resolucion - fecha_sentencia_recurrida).days

            duplicate_of = None
            if clean_hash in exact_representatives:
                duplicate_of = exact_representatives[clean_hash]
                if clean_hash not in exact_group_started:
                    exact_duplicate_groups += 1
                    exact_group_started.add(clean_hash)
            else:
                for candidate in near_candidates[near_key]:
                    if abs(token_count - candidate["token_count"]) > 5:
                        continue
                    score = jaccard_similarity(shingles, candidate["shingles"])
                    if score >= args.near_dup_threshold:
                        duplicate_of = candidate["fragmento_id"]
                        if candidate["fragmento_id"] not in near_group_started:
                            near_duplicate_groups += 1
                            near_group_started.add(candidate["fragmento_id"])
                        break

            is_duplicate = duplicate_of is not None
            if is_duplicate:
                duplicate_representatives.add(duplicate_of)
                duplicates_flagged += 1
            else:
                exact_representatives[clean_hash] = fragmento_id
                if len(near_candidates[near_key]) < args.near_dup_max_bucket:
                    near_candidates[near_key].append({
                        "fragmento_id": fragmento_id,
                        "token_count": token_count,
                        "shingles": shingles,
                    })
                if embedding is not None:
                    dedup_rows_with_embeddings += 1

            if within_range:
                within_target_window_hits += 1

            tribunal_counter.update([tribunal_id] if tribunal_id else [])
            label_counter.update([label])
            tipos_fragmento_set.add(tipo_fragmento or "SIN_TIPO")
            tipos_proceso_set.add(tipo_proceso or "SIN_TIPO_PROCESO")
            materias_set.update(materias_list)

            stage_records.append({
                "row_idx": row_idx,
                "fragmento_id": fragmento_id,
                "sentencia_id": sentencia_id,
                "tipo_fragmento": tipo_fragmento or "SIN_TIPO",
                "orden": int(orden) if orden is not None else -1,
                "label": label,
                "texto_limpio": texto_limpio,
                "clean_hash": clean_hash,
                "near_key": near_key,
                "token_count": token_count,
                "char_count": char_count,
                "within_target_window": within_range,
                "materias_json": json.dumps(materias_list, ensure_ascii=False),
                "primary_materia": primary_materia,
                "tipo_proceso": tipo_proceso or "SIN_TIPO_PROCESO",
                "tribunal_id": tribunal_id,
                "fecha_resolucion": fecha_resolucion,
                "fecha_sentencia_recurrida": fecha_sentencia_recurrida,
                "anio": anio,
                "mes": mes,
                "dia_semana": dia_semana,
                "dias_desde_recurrida": dias_desde_recurrida,
                "has_embedding": embedding is not None,
                "is_duplicate": is_duplicate,
                "duplicate_of": duplicate_of,
            })

            row_idx += 1
            rows_loaded += 1

        stage_part_index = write_stage_batch(paths["stage_parts_dir"], stage_part_index, stage_records)
        stage_records.clear()
        log(f"[stage] Procesados {rows_loaded:,} fragmentos...")

    cursor.close()
    conn.close()

    if embedding_memmap is not None:
        embedding_memmap.flush()
        del embedding_memmap

    paths["duplicate_reps_path"].write_text(
        "\n".join(sorted(duplicate_representatives)),
        encoding="utf-8",
    )

    stage_info = {
        "source_db": describe_db_target(),
        "rows_loaded": int(rows_loaded),
        "rows_with_embeddings": int(rows_with_embeddings),
        "rows_without_embeddings": int(rows_without_embeddings),
        "dedup_rows_with_embeddings": int(dedup_rows_with_embeddings),
        "exact_duplicate_groups": int(exact_duplicate_groups),
        "near_duplicate_groups": int(near_duplicate_groups),
        "embedding_duplicate_clusters": int(embedding_duplicate_clusters),
        "duplicates_flagged": int(duplicates_flagged),
        "within_target_window_ratio": float(within_target_window_hits / rows_loaded) if rows_loaded else 0.0,
        "label_distribution": dict(sorted(label_counter.items())),
        "observed_materias": sorted(materias_set),
        "observed_tipos_fragmento": sorted(tipos_fragmento_set),
        "observed_tipos_proceso": sorted(tipos_proceso_set),
        "top_tribunales": [tribunal for tribunal, _ in tribunal_counter.most_common(args.top_tribunales)],
        "embedding_dim": int(embedding_dim),
        "stage_part_count": int(max(stage_part_index - 1, 0)),
        "candidate_rows": int(candidate_rows),
    }

    paths["stage_info_path"].write_text(
        json.dumps(stage_info, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return stage_info


def fit_incremental_pca(stage_info: dict, args: argparse.Namespace, paths: dict[str, Path]) -> tuple[IncrementalPCA | None, dict]:
    if args.skip_embeddings or args.pca_components <= 0 or stage_info["dedup_rows_with_embeddings"] <= 1:
        return None, {"pca_components": 0, "pca_explained_variance": 0.0}

    component_count = min(
        int(args.pca_components),
        int(stage_info["embedding_dim"]),
        int(stage_info["dedup_rows_with_embeddings"]),
    )
    if component_count <= 0:
        return None, {"pca_components": 0, "pca_explained_variance": 0.0}

    embeddings = open_embedding_memmap(
        paths["embedding_path"],
        rows=int(stage_info["rows_loaded"]),
        dim=int(stage_info["embedding_dim"]),
        mode="r",
    )
    pca = IncrementalPCA(n_components=component_count)
    buffer_list: list[np.ndarray] = []
    fitted_once = False

    min_batch = max(component_count, int(args.ipca_batch_size))
    for part_path in get_stage_part_paths(paths["stage_parts_dir"]):
        frame = pd.read_parquet(part_path)
        mask = (~frame["is_duplicate"]) & frame["has_embedding"]
        if not mask.any():
            continue
        row_ids = frame.loc[mask, "row_idx"].to_numpy(dtype=np.int64)
        buffer_list.append(np.asarray(embeddings[row_ids], dtype=np.float32))
        pending = sum(chunk.shape[0] for chunk in buffer_list)
        if pending >= min_batch:
            fitted_once, buffer_list = flush_partial_fit_buffer(pca, buffer_list, component_count, fitted_once)
            log(f"[ipca] partial_fit acumulado: {pending:,} muestras")

    fitted_once, buffer_list = flush_partial_fit_buffer(pca, buffer_list, component_count, fitted_once)
    del embeddings

    if not fitted_once:
        return None, {"pca_components": 0, "pca_explained_variance": 0.0}

    return pca, {
        "pca_components": int(component_count),
        "pca_explained_variance": float(np.sum(pca.explained_variance_ratio_)),
    }


def fit_incremental_kmeans(
    stage_info: dict,
    args: argparse.Namespace,
    paths: dict[str, Path],
    pca_model: IncrementalPCA | None,
) -> tuple[MiniBatchKMeans | None, dict]:
    if args.skip_embeddings or args.clusters <= 0 or stage_info["dedup_rows_with_embeddings"] <= 1:
        return None, {"cluster_count": 0}

    cluster_count = min(int(args.clusters), int(stage_info["dedup_rows_with_embeddings"]))
    if cluster_count <= 1:
        return None, {"cluster_count": 0}

    embeddings = open_embedding_memmap(
        paths["embedding_path"],
        rows=int(stage_info["rows_loaded"]),
        dim=int(stage_info["embedding_dim"]),
        mode="r",
    )

    model = MiniBatchKMeans(
        n_clusters=cluster_count,
        batch_size=max(cluster_count, int(args.kmeans_batch_size)),
        random_state=args.random_state,
        n_init=10,
    )
    buffer_list: list[np.ndarray] = []
    fitted_once = False

    min_batch = max(cluster_count, int(args.kmeans_batch_size))
    for part_path in get_stage_part_paths(paths["stage_parts_dir"]):
        frame = pd.read_parquet(part_path)
        mask = (~frame["is_duplicate"]) & frame["has_embedding"]
        if not mask.any():
            continue
        row_ids = frame.loc[mask, "row_idx"].to_numpy(dtype=np.int64)
        batch = np.asarray(embeddings[row_ids], dtype=np.float32)
        if pca_model is not None:
            batch = pca_model.transform(batch)
        buffer_list.append(batch)
        pending = sum(chunk.shape[0] for chunk in buffer_list)
        if pending >= min_batch:
            fitted_once, buffer_list = flush_partial_fit_buffer(model, buffer_list, cluster_count, fitted_once)
            log(f"[kmeans] partial_fit acumulado: {pending:,} muestras")

    fitted_once, buffer_list = flush_partial_fit_buffer(model, buffer_list, cluster_count, fitted_once)
    del embeddings

    if not fitted_once:
        return None, {"cluster_count": 0}
    return model, {"cluster_count": int(cluster_count)}


def build_feature_batches_incremental(
    frame: pd.DataFrame,
    schema: CategorySchema,
    duplicate_representatives: set[str],
    embeddings: np.memmap | None,
    pca_model: IncrementalPCA | None,
    kmeans_model: MiniBatchKMeans | None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    work_df = frame.copy()
    work_df["materias"] = decode_materias_json(work_df["materias_json"])
    work_df["materia_count"] = work_df["materias"].apply(len)
    top_tribunal_ids = set(schema.top_tribunales)
    work_df["tribunal_bucket"] = work_df["tribunal_id"].apply(
        lambda value: value if value in top_tribunal_ids else "OTRO_TRIBUNAL"
    )

    numeric = work_df[
        [
            "orden",
            "token_count",
            "char_count",
            "within_target_window",
            "materia_count",
            "anio",
            "mes",
            "dia_semana",
            "dias_desde_recurrida",
        ]
    ].copy()
    numeric["within_target_window"] = numeric["within_target_window"].astype(np.int8)
    numeric = numeric.fillna(-1)

    materia_frame = build_multi_hot_frame(work_df["materias"].tolist(), schema.materias, "materia", work_df.index)
    tipo_fragmento_frame = build_one_hot_frame(
        work_df["tipo_fragmento"].tolist(),
        schema.tipos_fragmento,
        "tipo_fragmento",
        work_df.index,
    )
    tipo_proceso_frame = build_one_hot_frame(
        work_df["tipo_proceso"].tolist(),
        schema.tipos_proceso,
        "tipo_proceso",
        work_df.index,
    )
    tribunal_categories = schema.top_tribunales + ["OTRO_TRIBUNAL"]
    tribunal_frame = build_one_hot_frame(
        work_df["tribunal_bucket"].tolist(),
        tribunal_categories,
        "tribunal",
        work_df.index,
    )

    cluster_series = pd.Series(-1, index=work_df.index, dtype=np.int32)
    reduced_frame = pd.DataFrame(index=work_df.index)

    if embeddings is not None and work_df["has_embedding"].any():
        row_ids = work_df.loc[work_df["has_embedding"], "row_idx"].to_numpy(dtype=np.int64)
        vectors = np.asarray(embeddings[row_ids], dtype=np.float32)
        reduced_vectors = None
        if pca_model is not None:
            reduced_vectors = pca_model.transform(vectors)
            reduced_columns = [f"pca_{index:03d}" for index in range(reduced_vectors.shape[1])]
            reduced_frame = pd.DataFrame(
                reduced_vectors,
                index=work_df.index[work_df["has_embedding"]],
                columns=reduced_columns,
            )
        if kmeans_model is not None:
            cluster_input = reduced_vectors if reduced_vectors is not None else vectors
            predicted = kmeans_model.predict(cluster_input).astype(np.int32)
            cluster_series.loc[work_df.index[work_df["has_embedding"]]] = predicted

    duplicate_group = []
    for fragmento_id, is_duplicate, duplicate_of in zip(
        work_df["fragmento_id"].tolist(),
        work_df["is_duplicate"].tolist(),
        work_df["duplicate_of"].tolist(),
        strict=True,
    ):
        if is_duplicate:
            duplicate_group.append(duplicate_of)
        elif fragmento_id in duplicate_representatives:
            duplicate_group.append(fragmento_id)
        else:
            duplicate_group.append(None)

    full_df = work_df[
        [
            "row_idx",
            "fragmento_id",
            "sentencia_id",
            "tipo_fragmento",
            "orden",
            "label",
            "texto_limpio",
            "clean_hash",
            "near_key",
            "token_count",
            "char_count",
            "within_target_window",
            "materias",
            "primary_materia",
            "tipo_proceso",
            "tribunal_id",
            "fecha_resolucion",
            "fecha_sentencia_recurrida",
            "anio",
            "mes",
            "dia_semana",
            "dias_desde_recurrida",
            "has_embedding",
            "is_duplicate",
            "duplicate_of",
        ]
    ].copy()
    full_df["duplicate_group"] = duplicate_group
    full_df["cluster_id"] = cluster_series.astype(np.int32)
    if not reduced_frame.empty:
        full_df = pd.concat([full_df, reduced_frame], axis=1)

    dedup_mask = ~work_df["is_duplicate"]
    model_df = pd.concat(
        [
            work_df.loc[dedup_mask, ["fragmento_id", "sentencia_id", "label"]].reset_index(drop=True),
            numeric.loc[dedup_mask].reset_index(drop=True),
            tipo_fragmento_frame.loc[dedup_mask].reset_index(drop=True),
            tipo_proceso_frame.loc[dedup_mask].reset_index(drop=True),
            tribunal_frame.loc[dedup_mask].reset_index(drop=True),
            materia_frame.loc[dedup_mask].reset_index(drop=True),
        ],
        axis=1,
    )
    if not reduced_frame.empty:
        model_df = pd.concat([model_df, reduced_frame.loc[dedup_mask].reset_index(drop=True).fillna(0)], axis=1)
    model_df["cluster_id"] = cluster_series.loc[dedup_mask].reset_index(drop=True).astype(np.int32)
    return full_df, model_df


def run_pipeline_incremental(args: argparse.Namespace) -> tuple[dict, dict[str, str]]:
    notes: list[str] = []
    nlp = None
    if args.lemmatize or args.remove_stopwords:
        nlp, nlp_notes = load_spacy_model(args.spacy_model)
        notes.extend(nlp_notes)

    if args.balance != "none":
        notes.append("El balanceo se omite en modo incremental; usa class_weight o una muestra en modo memory.")
    if args.skip_embeddings:
        notes.append("Embeddings omitidos por --skip-embeddings.")
        args.pca_components = 0
        args.clusters = 0
    else:
        notes.append("DBSCAN se omite en modo incremental para no cargar todo el espacio vectorial en memoria.")

    paths = prepare_incremental_paths(args)
    stage_info = stage_incremental_corpus(args, nlp, paths)
    if stage_info["rows_loaded"] == 0:
        raise RuntimeError("No se encontraron fragmentos utilizables.")

    pca_model, pca_summary = fit_incremental_pca(stage_info, args, paths)
    kmeans_model, cluster_summary = fit_incremental_kmeans(stage_info, args, paths, pca_model)

    schema = CategorySchema(
        materias=stage_info["observed_materias"],
        tipos_fragmento=stage_info["observed_tipos_fragmento"],
        tipos_proceso=stage_info["observed_tipos_proceso"],
        top_tribunales=stage_info["top_tribunales"],
    )
    duplicate_representatives = load_duplicate_representatives(paths["duplicate_reps_path"])

    embeddings = None
    if not args.skip_embeddings and stage_info["rows_with_embeddings"] > 0 and stage_info["embedding_dim"] > 0:
        embeddings = open_embedding_memmap(
            paths["embedding_path"],
            rows=int(stage_info["rows_loaded"]),
            dim=int(stage_info["embedding_dim"]),
            mode="r",
        )

    persist_conn = None
    if args.persist_db:
        persist_conn = connect_db()
        ensure_feature_table(persist_conn)

    model_label_counter: Counter[str] = Counter()
    numeric_feature_count = 0
    exported_full_parts = 0
    exported_model_parts = 0

    for part_index, part_path in enumerate(get_stage_part_paths(paths["stage_parts_dir"]), start=1):
        frame = pd.read_parquet(part_path)
        full_df, model_df = build_feature_batches_incremental(
            frame,
            schema,
            duplicate_representatives,
            embeddings,
            pca_model,
            kmeans_model,
        )

        full_df.to_parquet(paths["full_parts_dir"] / f"part-{part_index:06d}.parquet", index=False)
        model_df.to_parquet(paths["model_parts_dir"] / f"part-{part_index:06d}.parquet", index=False)
        exported_full_parts += 1
        exported_model_parts += 1

        if persist_conn is not None:
            persist_features_batch(persist_conn, full_df)

        model_label_counter.update(model_df["label"].tolist())
        if numeric_feature_count == 0:
            numeric_feature_count = int(
                model_df.drop(columns=["fragmento_id", "sentencia_id", "label"]).shape[1]
            )
        log(f"[export] Partes generadas: {part_index:,}")

    if persist_conn is not None:
        persist_conn.close()
    if embeddings is not None:
        del embeddings

    summary = {
        "source_db": stage_info["source_db"],
        "rows_loaded": stage_info["rows_loaded"],
        "rows_with_embeddings": stage_info["rows_with_embeddings"],
        "rows_without_embeddings": stage_info["rows_without_embeddings"],
        "exact_duplicate_groups": stage_info["exact_duplicate_groups"],
        "near_duplicate_groups": stage_info["near_duplicate_groups"],
        "embedding_duplicate_clusters": stage_info["embedding_duplicate_clusters"],
        "duplicates_flagged": stage_info["duplicates_flagged"],
        "top_tribunales": stage_info["top_tribunales"],
        "model_rows_after_dedup": int(sum(model_label_counter.values())),
        "numeric_feature_count": int(numeric_feature_count),
        "pca_components": pca_summary["pca_components"],
        "pca_explained_variance": pca_summary["pca_explained_variance"],
        "cluster_count": cluster_summary["cluster_count"],
        "balance_method_applied": "not_supported_incremental" if args.balance != "none" else "none",
        "label_distribution": stage_info["label_distribution"],
        "model_label_distribution": dict(sorted(model_label_counter.items())),
        "class_weights_suggestion": compute_class_weights_from_counts(dict(model_label_counter)),
        "within_target_window_ratio": stage_info["within_target_window_ratio"],
        "notes": notes,
    }
    if args.persist_db:
        summary["persisted_table"] = "fragmentos_features_ml"

    paths["summary_path"].write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    exported = {
        "full_parts_dir": str(paths["full_parts_dir"]),
        "model_parts_dir": str(paths["model_parts_dir"]),
        "summary_path": str(paths["summary_path"]),
        "stage_dir": str(paths["stage_dir"]),
    }

    if args.cleanup_stage:
        shutil.rmtree(paths["stage_dir"])
        exported["stage_dir"] = "(eliminado por --cleanup-stage)"

    return summary, exported


def main() -> int:
    sys.stdout.reconfigure(encoding="utf-8")
    args = parse_args()
    mode = choose_pipeline_mode(args)

    log("=" * 72)
    log("PREPROCESAMIENTO ML SIN FINE-TUNING")
    log("=" * 72)
    log(f"Base de datos: {describe_db_target()}")
    log(f"Modo: {mode}")
    log(f"Max rows: {args.max_rows if args.max_rows else 'todo el corpus'}")
    log(f"Salida: {args.output_dir}")

    if mode == "memory":
        artifacts = run_pipeline_memory(args)
        exported = export_artifacts(
            artifacts,
            output_dir=Path(args.output_dir),
            output_prefix=args.output_prefix,
        )
        summary = artifacts.summary
    else:
        summary, exported = run_pipeline_incremental(args)

    log("")
    log("Resumen:")
    log(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    log("")
    log("Artefactos exportados:")
    for key, value in exported.items():
        log(f"  - {key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
