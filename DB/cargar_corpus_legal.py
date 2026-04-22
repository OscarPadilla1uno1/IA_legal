import argparse
import hashlib
import json
import re
import sys
import unicodedata
import uuid
from pathlib import Path

import psycopg2

from db_config import connect_db

sys.stdout.reconfigure(encoding="utf-8")

NAMESPACE = uuid.UUID("87c0c05b-0ca5-4694-b5db-0cb1126e07d4")


def normalize_text(value):
    if value is None:
        return None
    value = unicodedata.normalize("NFKD", str(value))
    value = "".join(char for char in value if not unicodedata.combining(char))
    value = re.sub(r"\s+", " ", value).strip().lower()
    return value or None


def stable_uuid(*parts):
    raw = "|".join("" if part is None else str(part) for part in parts)
    return str(uuid.uuid5(NAMESPACE, raw))


def split_sentences(text):
    text = re.sub(r"\s+", " ", text or "").strip()
    if not text:
        return []
    pieces = re.split(r"(?<=[.!?;:])\s+", text)
    return [piece.strip() for piece in pieces if piece.strip()]


def chunk_text(text, target_words=180, overlap_words=45):
    sentences = split_sentences(text)
    if not sentences:
        return []

    chunks = []
    current = []
    current_words = 0

    for sentence in sentences:
        words = len(sentence.split())
        if current and current_words + words > target_words:
            chunks.append(" ".join(current))

            overlap = []
            overlap_count = 0
            for item in reversed(current):
                item_words = len(item.split())
                if overlap_count + item_words <= overlap_words or not overlap:
                    overlap.insert(0, item)
                    overlap_count += item_words
                else:
                    break

            current = overlap
            current_words = overlap_count

        current.append(sentence)
        current_words += words

    if current:
        chunks.append(" ".join(current))

    return chunks


def load_payload(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def ensure_list(payload):
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get("legislaciones", [])
    raise ValueError("El archivo JSON debe contener una lista o una clave 'legislaciones'.")


def upsert(cursor, sql, params):
    cursor.execute(sql, params)


def insert_aliases(cursor, ley_id, aliases):
    seen = set()
    for index, alias in enumerate(aliases):
        alias_clean = re.sub(r"\s+", " ", str(alias or "")).strip()
        alias_norm = normalize_text(alias_clean)
        if not alias_norm or alias_norm in seen:
            continue
        seen.add(alias_norm)
        alias_id = stable_uuid("alias", ley_id, alias_norm)
        upsert(
            cursor,
            """
            INSERT INTO leyes_alias (id, ley_id, alias, alias_normalizado, es_principal)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ley_id, alias_normalizado) DO UPDATE
            SET alias = EXCLUDED.alias,
                es_principal = leyes_alias.es_principal OR EXCLUDED.es_principal;
            """,
            (alias_id, ley_id, alias_clean, alias_norm, index == 0),
        )


def insert_fragments(
    cursor,
    ley_id,
    version_id,
    articulo_id,
    articulo_etiqueta,
    texto_oficial,
    nodo_id=None,
    tipo_fragmento=None,
):
    fragments = chunk_text(texto_oficial)
    if not fragments:
        return 0

    total = 0
    for order_index, fragment in enumerate(fragments, start=1):
        tipo = tipo_fragmento or ("articulo" if len(fragments) == 1 else "articulo_chunk")
        fragment_anchor = articulo_id or nodo_id or articulo_etiqueta
        fragment_id = stable_uuid("frag", fragment_anchor, order_index, tipo)
        contenido = f"{articulo_etiqueta}. {fragment}" if not fragment.startswith(articulo_etiqueta) else fragment
        upsert(
            cursor,
            """
            INSERT INTO fragmentos_normativos (
                id, ley_id, ley_version_id, articulo_id, nodo_id, tipo_fragmento, contenido, orden
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE
            SET tipo_fragmento = EXCLUDED.tipo_fragmento,
                contenido = EXCLUDED.contenido,
                orden = EXCLUDED.orden,
                ley_id = EXCLUDED.ley_id,
                ley_version_id = EXCLUDED.ley_version_id,
                articulo_id = EXCLUDED.articulo_id,
                nodo_id = EXCLUDED.nodo_id,
                embedding_fragmento = NULL,
                tsv_contenido = NULL;
            """,
            (fragment_id, ley_id, version_id, articulo_id, nodo_id, tipo, contenido, order_index),
        )
        total += 1
    return total


def insert_node(cursor, version_id, parent_id, node, order_index):
    node_type = node["tipo_nodo"]
    label = node["etiqueta"]
    identifier = node.get("identificador")
    node_id = node.get("id") or stable_uuid("nodo", version_id, parent_id, node_type, identifier or label)
    title = node.get("titulo")
    text = re.sub(r"\s+", " ", node.get("texto", "")).strip() or None

    upsert(
        cursor,
        """
        INSERT INTO nodos_normativos (
            id, ley_version_id, nodo_padre_id, tipo_nodo, etiqueta, identificador,
            titulo, texto, texto_normalizado, orden, metadata
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (id) DO UPDATE
        SET ley_version_id = EXCLUDED.ley_version_id,
            nodo_padre_id = EXCLUDED.nodo_padre_id,
            tipo_nodo = EXCLUDED.tipo_nodo,
            etiqueta = EXCLUDED.etiqueta,
            identificador = EXCLUDED.identificador,
            titulo = EXCLUDED.titulo,
            texto = EXCLUDED.texto,
            texto_normalizado = EXCLUDED.texto_normalizado,
            orden = EXCLUDED.orden,
            metadata = EXCLUDED.metadata;
        """,
        (
            node_id,
            version_id,
            parent_id,
            node_type,
            label,
            identifier,
            title,
            text,
            normalize_text(text),
            node.get("orden", order_index),
            json.dumps(node.get("metadata", {}), ensure_ascii=False),
        ),
    )
    return node_id, text


def process_nodes(cursor, ley_id, version_id, nodes, counters, parent_id=None):
    for order_index, node in enumerate(nodes, start=1):
        node_id, node_text = insert_node(cursor, version_id, parent_id, node, order_index)
        counters["nodos"] += 1
        if node_text:
            counters["fragmentos"] += insert_fragments(
                cursor,
                ley_id=ley_id,
                version_id=version_id,
                articulo_id=None,
                articulo_etiqueta=node.get("etiqueta") or node.get("titulo") or node["tipo_nodo"],
                texto_oficial=node_text,
                nodo_id=node_id,
                tipo_fragmento=f"nodo_{node['tipo_nodo']}",
            )
        child_nodes = node.get("nodos", [])
        if child_nodes:
            process_nodes(cursor, ley_id, version_id, child_nodes, counters, parent_id=node_id)


def process_dataset(cursor, dataset):
    counters = {
        "legislaciones": 0,
        "leyes": 0,
        "versiones": 0,
        "articulos": 0,
        "nodos": 0,
        "fragmentos": 0,
    }

    for family in dataset:
        family_name = family["nombre"]
        family_id = family.get("id") or stable_uuid("legislacion", family.get("codigo"), family_name)
        upsert(
            cursor,
            """
            INSERT INTO legislaciones (id, codigo, nombre, descripcion)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE
            SET codigo = EXCLUDED.codigo,
                nombre = EXCLUDED.nombre,
                descripcion = EXCLUDED.descripcion;
            """,
            (family_id, family.get("codigo"), family_name, family.get("descripcion")),
        )
        counters["legislaciones"] += 1

        for law in family.get("leyes", []):
            law_name = law["nombre_oficial"]
            ley_id = law.get("id") or stable_uuid("ley", family_id, law_name)
            upsert(
                cursor,
                """
                INSERT INTO leyes (
                    id, legislacion_id, tipo_norma, nombre_oficial, nombre_corto, sigla, ambito,
                    autoridad_emisora, estado_vigencia, fecha_publicacion, fecha_vigencia,
                    fecha_derogacion, metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (id) DO UPDATE
                SET legislacion_id = EXCLUDED.legislacion_id,
                    tipo_norma = EXCLUDED.tipo_norma,
                    nombre_oficial = EXCLUDED.nombre_oficial,
                    nombre_corto = EXCLUDED.nombre_corto,
                    sigla = EXCLUDED.sigla,
                    ambito = EXCLUDED.ambito,
                    autoridad_emisora = EXCLUDED.autoridad_emisora,
                    estado_vigencia = EXCLUDED.estado_vigencia,
                    fecha_publicacion = EXCLUDED.fecha_publicacion,
                    fecha_vigencia = EXCLUDED.fecha_vigencia,
                    fecha_derogacion = EXCLUDED.fecha_derogacion,
                    metadata = EXCLUDED.metadata;
                """,
                (
                    ley_id,
                    family_id,
                    law.get("tipo_norma", "ley"),
                    law_name,
                    law.get("nombre_corto"),
                    law.get("sigla"),
                    law.get("ambito"),
                    law.get("autoridad_emisora"),
                    law.get("estado_vigencia", "vigente"),
                    law.get("fecha_publicacion"),
                    law.get("fecha_vigencia"),
                    law.get("fecha_derogacion"),
                    json.dumps(law.get("metadata", {}), ensure_ascii=False),
                ),
            )
            counters["leyes"] += 1

            aliases = [law_name]
            if law.get("nombre_corto"):
                aliases.append(law["nombre_corto"])
            if law.get("sigla"):
                aliases.append(law["sigla"])
            aliases.extend(law.get("aliases", []))
            insert_aliases(cursor, ley_id, aliases)

            for version in law.get("versiones", []):
                version_id = version.get("id") or stable_uuid(
                    "version", ley_id, version.get("version_label"), version.get("fecha_inicio_vigencia")
                )
                upsert(
                    cursor,
                    """
                    INSERT INTO leyes_versiones (
                        id, ley_id, version_label, fecha_inicio_vigencia, fecha_fin_vigencia,
                        es_vigente, texto_consolidado, fuente_url, notas
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE
                    SET ley_id = EXCLUDED.ley_id,
                        version_label = EXCLUDED.version_label,
                        fecha_inicio_vigencia = EXCLUDED.fecha_inicio_vigencia,
                        fecha_fin_vigencia = EXCLUDED.fecha_fin_vigencia,
                        es_vigente = EXCLUDED.es_vigente,
                        texto_consolidado = EXCLUDED.texto_consolidado,
                        fuente_url = EXCLUDED.fuente_url,
                        notas = EXCLUDED.notas;
                    """,
                    (
                        version_id,
                        ley_id,
                        version.get("version_label"),
                        version.get("fecha_inicio_vigencia"),
                        version.get("fecha_fin_vigencia"),
                        version.get("es_vigente", True),
                        version.get("texto_consolidado"),
                        version.get("fuente_url"),
                        version.get("notas"),
                    ),
                )
                counters["versiones"] += 1

                root_nodes = version.get("nodos", [])
                if root_nodes:
                    process_nodes(cursor, ley_id, version_id, root_nodes, counters)

                for order_index, article in enumerate(version.get("articulos", []), start=1):
                    article_number = str(article["articulo_numero"]).strip()
                    article_label = article.get("articulo_etiqueta") or f"Artículo {article_number}"
                    text = re.sub(r"\s+", " ", article["texto_oficial"]).strip()
                    article_id = article.get("id") or stable_uuid(
                        "articulo",
                        version_id,
                        article_label,
                        article.get("numeral"),
                        article.get("literal"),
                    )
                    upsert(
                        cursor,
                        """
                        INSERT INTO articulos_ley (
                            id, ley_version_id, articulo_numero, articulo_etiqueta, numeral, literal,
                            rubro, texto_oficial, texto_normalizado, orden, hash_contenido, metadata
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        ON CONFLICT (id) DO UPDATE
                        SET ley_version_id = EXCLUDED.ley_version_id,
                            articulo_numero = EXCLUDED.articulo_numero,
                            articulo_etiqueta = EXCLUDED.articulo_etiqueta,
                            numeral = EXCLUDED.numeral,
                            literal = EXCLUDED.literal,
                            rubro = EXCLUDED.rubro,
                            texto_oficial = EXCLUDED.texto_oficial,
                            texto_normalizado = EXCLUDED.texto_normalizado,
                            orden = EXCLUDED.orden,
                            hash_contenido = EXCLUDED.hash_contenido,
                            metadata = EXCLUDED.metadata;
                        """,
                        (
                            article_id,
                            version_id,
                            article_number,
                            article_label,
                            article.get("numeral"),
                            article.get("literal"),
                            article.get("rubro"),
                            text,
                            normalize_text(text),
                            article.get("orden", order_index),
                            hashlib.sha256(text.encode("utf-8")).hexdigest(),
                            json.dumps(article.get("metadata", {}), ensure_ascii=False),
                        ),
                    )
                    counters["articulos"] += 1
                    counters["fragmentos"] += insert_fragments(
                        cursor,
                        ley_id=ley_id,
                        version_id=version_id,
                        articulo_id=article_id,
                        articulo_etiqueta=article_label,
                        texto_oficial=text,
                    )

    return counters


def main():
    parser = argparse.ArgumentParser(description="Carga leyes canónicas de Honduras a PostgreSQL.")
    parser.add_argument("--input", required=True, help="Ruta al JSON del corpus legal canónico.")
    args = parser.parse_args()

    path = Path(args.input).resolve()
    payload = load_payload(path)
    dataset = ensure_list(payload)

    conn = connect_db()
    cursor = conn.cursor()

    try:
        counters = process_dataset(cursor, dataset)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    print("Carga completada.")
    for key, value in counters.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
