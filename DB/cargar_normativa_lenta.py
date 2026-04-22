import argparse
import hashlib
import json
import re
import sys
import unicodedata
import uuid
from pathlib import Path

import psycopg2

sys.stdout.reconfigure(encoding="utf-8")

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432",
}

NAMESPACE = uuid.UUID("4f6d838d-34a3-48c5-89ef-ffbc79fd6cf6")


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


def hash_text(value):
    if not value:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def load_payload(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def ensure_list(payload):
    if isinstance(payload, dict):
        return payload.get("documentos", [])
    if isinstance(payload, list):
        return payload
    raise ValueError("El archivo JSON debe contener una lista o una clave 'documentos'.")


def upsert(cursor, sql, params):
    cursor.execute(sql, params)


def get_legislacion_id(cursor, family):
    if not family:
        return None

    if isinstance(family, str):
        family = {"codigo": family, "nombre": family}

    codigo = family.get("codigo")
    nombre = family.get("nombre") or codigo
    if not nombre:
        return None

    cursor.execute(
        """
        SELECT id
        FROM legislaciones
        WHERE (%s IS NOT NULL AND codigo = %s)
           OR LOWER(nombre) = LOWER(%s)
        ORDER BY created_at ASC
        LIMIT 1
        """,
        (codigo, codigo, nombre),
    )
    row = cursor.fetchone()
    if row:
        return row[0]

    family_id = family.get("id") or stable_uuid("legislacion_lenta", codigo or nombre)
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
        (family_id, codigo, nombre, family.get("descripcion")),
    )
    return family_id


def get_tipo_documento_id(cursor, codigo):
    cursor.execute("SELECT id FROM norma_tipos_documento WHERE codigo = %s", (codigo,))
    row = cursor.fetchone()
    if not row:
        raise ValueError(f"Tipo de documento no sembrado: {codigo}")
    return row[0]


def get_fuente_id(cursor, fuente):
    if not fuente:
        return None
    cursor.execute("SELECT id FROM norma_fuentes WHERE codigo = %s", (fuente["codigo"],))
    row = cursor.fetchone()
    if row:
        return row[0]

    fuente_id = fuente.get("id") or stable_uuid("fuente", fuente["codigo"])
    upsert(
        cursor,
        """
        INSERT INTO norma_fuentes (id, codigo, nombre, base_url, tipo_fuente, metadata)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (codigo) DO UPDATE
        SET nombre = EXCLUDED.nombre,
            base_url = EXCLUDED.base_url,
            tipo_fuente = EXCLUDED.tipo_fuente,
            metadata = EXCLUDED.metadata;
        """,
        (
            fuente_id,
            fuente["codigo"],
            fuente.get("nombre") or fuente["codigo"],
            fuente.get("base_url"),
            fuente.get("tipo_fuente"),
            json.dumps(fuente.get("metadata", {}), ensure_ascii=False),
        ),
    )
    return fuente_id


def insert_aliases(cursor, documento_id, aliases):
    seen = set()
    for index, alias in enumerate(aliases):
        alias_clean = re.sub(r"\s+", " ", str(alias or "")).strip()
        alias_norm = normalize_text(alias_clean)
        if not alias_norm or alias_norm in seen:
            continue
        seen.add(alias_norm)
        alias_id = stable_uuid("norma_alias", documento_id, alias_norm)
        upsert(
            cursor,
            """
            INSERT INTO norma_aliases (id, documento_id, alias, alias_normalizado, es_principal)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (documento_id, alias_normalizado) DO UPDATE
            SET alias = EXCLUDED.alias,
                es_principal = norma_aliases.es_principal OR EXCLUDED.es_principal;
            """,
            (alias_id, documento_id, alias_clean, alias_norm, index == 0),
        )


def insert_fragments(cursor, documento_id, version_id, node_id, anchor, text, fragment_type, metadata=None):
    fragments = chunk_text(text)
    total = 0
    for order_index, fragment in enumerate(fragments, start=1):
        fragment_id = stable_uuid("norma_fragmento", node_id, order_index, fragment_type)
        contenido = f"{anchor}. {fragment}" if anchor and not fragment.startswith(anchor) else fragment
        upsert(
            cursor,
            """
            INSERT INTO norma_fragmentos (
                id, documento_id, version_id, nodo_id, tipo_fragmento, contenido, orden, metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (id) DO UPDATE
            SET contenido = EXCLUDED.contenido,
                tipo_fragmento = EXCLUDED.tipo_fragmento,
                orden = EXCLUDED.orden,
                metadata = EXCLUDED.metadata,
                documento_id = EXCLUDED.documento_id,
                version_id = EXCLUDED.version_id,
                nodo_id = EXCLUDED.nodo_id,
                embedding_fragmento = NULL,
                tsv_contenido = NULL;
            """,
            (
                fragment_id,
                documento_id,
                version_id,
                node_id,
                fragment_type,
                contenido,
                order_index,
                json.dumps(metadata or {}, ensure_ascii=False),
            ),
        )
        total += 1
    return total


def insert_node(cursor, version_id, parent_id, depth, parent_path, node, order_index):
    node_ref = node.get("ref")
    node_type = node["tipo_nodo"]
    label = node["etiqueta"]
    identifier = node.get("identificador")
    title = node.get("titulo")
    article_number = node.get("articulo_numero")
    text = re.sub(r"\s+", " ", node.get("texto", "")).strip() or None
    path_bits = [part for part in [parent_path, label] if part]
    path_text = " > ".join(path_bits)
    node_id = node.get("id") or stable_uuid(
        "norma_nodo",
        version_id,
        node_ref or node_type,
        parent_id,
        identifier or label,
    )

    upsert(
        cursor,
        """
        INSERT INTO norma_nodos (
            id, version_id, nodo_padre_id, ref_carga, tipo_nodo, etiqueta, identificador,
            titulo, articulo_numero, texto, texto_normalizado, orden, profundidad, path_text, metadata
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (id) DO UPDATE
        SET version_id = EXCLUDED.version_id,
            nodo_padre_id = EXCLUDED.nodo_padre_id,
            ref_carga = EXCLUDED.ref_carga,
            tipo_nodo = EXCLUDED.tipo_nodo,
            etiqueta = EXCLUDED.etiqueta,
            identificador = EXCLUDED.identificador,
            titulo = EXCLUDED.titulo,
            articulo_numero = EXCLUDED.articulo_numero,
            texto = EXCLUDED.texto,
            texto_normalizado = EXCLUDED.texto_normalizado,
            orden = EXCLUDED.orden,
            profundidad = EXCLUDED.profundidad,
            path_text = EXCLUDED.path_text,
            metadata = EXCLUDED.metadata;
        """,
        (
            node_id,
            version_id,
            parent_id,
            node_ref,
            node_type,
            label,
            identifier,
            title,
            article_number,
            text,
            normalize_text(text),
            node.get("orden", order_index),
            depth,
            path_text,
            json.dumps(node.get("metadata", {}), ensure_ascii=False),
        ),
    )

    return node_id, text, path_text


def process_nodes(cursor, documento_id, version_id, nodes, counters, parent_id=None, depth=0, parent_path=None):
    refs = {}
    for order_index, node in enumerate(nodes, start=1):
        node_id, node_text, path_text = insert_node(
            cursor,
            version_id=version_id,
            parent_id=parent_id,
            depth=depth,
            parent_path=parent_path,
            node=node,
            order_index=order_index,
        )
        counters["nodos"] += 1
        if node.get("ref"):
            refs[node["ref"]] = node_id
        if node_text:
            fragment_type = f"nodo_{node['tipo_nodo']}"
            anchor = node.get("etiqueta") or node.get("titulo") or node["tipo_nodo"]
            counters["fragmentos"] += insert_fragments(
                cursor,
                documento_id=documento_id,
                version_id=version_id,
                node_id=node_id,
                anchor=anchor,
                text=node_text,
                fragment_type=fragment_type,
                metadata=node.get("metadata"),
            )
        child_refs = process_nodes(
            cursor,
            documento_id=documento_id,
            version_id=version_id,
            nodes=node.get("nodos", []),
            counters=counters,
            parent_id=node_id,
            depth=depth + 1,
            parent_path=path_text,
        )
        refs.update(child_refs)
    return refs


def process_dataset(cursor, payload):
    counters = {
        "documentos": 0,
        "versiones": 0,
        "nodos": 0,
        "fragmentos": 0,
        "relaciones": 0,
    }

    documents = ensure_list(payload)
    doc_refs = {}
    version_refs = {}
    node_refs = {}
    pending_relations = []

    for source in payload.get("fuentes", []) if isinstance(payload, dict) else []:
        get_fuente_id(cursor, source)

    for document in documents:
        doc_ref = document.get("ref")
        tipo_documento_id = get_tipo_documento_id(cursor, document["tipo_documento"])
        legislacion_id = get_legislacion_id(cursor, document.get("legislacion"))

        parent_document_id = None
        if document.get("documento_padre_ref"):
            parent_document_id = doc_refs.get(document["documento_padre_ref"])

        documento_id = document.get("id") or stable_uuid("norma_documento", doc_ref or document["nombre_oficial"])
        upsert(
            cursor,
            """
            INSERT INTO norma_documentos (
                id, ref_carga, legislacion_id, tipo_documento_id, documento_padre_id,
                nombre_oficial, nombre_corto, sigla, numero_oficial, autoridad_emisora,
                ambito, estado_vigencia, fecha_emision, fecha_publicacion, fecha_vigencia,
                fecha_derogacion, metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (id) DO UPDATE
            SET ref_carga = EXCLUDED.ref_carga,
                legislacion_id = EXCLUDED.legislacion_id,
                tipo_documento_id = EXCLUDED.tipo_documento_id,
                documento_padre_id = EXCLUDED.documento_padre_id,
                nombre_oficial = EXCLUDED.nombre_oficial,
                nombre_corto = EXCLUDED.nombre_corto,
                sigla = EXCLUDED.sigla,
                numero_oficial = EXCLUDED.numero_oficial,
                autoridad_emisora = EXCLUDED.autoridad_emisora,
                ambito = EXCLUDED.ambito,
                estado_vigencia = EXCLUDED.estado_vigencia,
                fecha_emision = EXCLUDED.fecha_emision,
                fecha_publicacion = EXCLUDED.fecha_publicacion,
                fecha_vigencia = EXCLUDED.fecha_vigencia,
                fecha_derogacion = EXCLUDED.fecha_derogacion,
                metadata = EXCLUDED.metadata;
            """,
            (
                documento_id,
                doc_ref,
                legislacion_id,
                tipo_documento_id,
                parent_document_id,
                document["nombre_oficial"],
                document.get("nombre_corto"),
                document.get("sigla"),
                document.get("numero_oficial"),
                document.get("autoridad_emisora"),
                document.get("ambito"),
                document.get("estado_vigencia", "vigente"),
                document.get("fecha_emision"),
                document.get("fecha_publicacion"),
                document.get("fecha_vigencia"),
                document.get("fecha_derogacion"),
                json.dumps(document.get("metadata", {}), ensure_ascii=False),
            ),
        )
        counters["documentos"] += 1
        if doc_ref:
            doc_refs[doc_ref] = documento_id

        aliases = [document["nombre_oficial"]]
        if document.get("nombre_corto"):
            aliases.append(document["nombre_corto"])
        if document.get("sigla"):
            aliases.append(document["sigla"])
        aliases.extend(document.get("aliases", []))
        insert_aliases(cursor, documento_id, aliases)

        for version in document.get("versiones", []):
            version_ref = version.get("ref")
            version_id = version.get("id") or stable_uuid(
                "norma_version",
                documento_id,
                version_ref or version.get("version_label"),
                version.get("fecha_inicio_vigencia"),
            )
            texto_consolidado = version.get("texto_consolidado")
            upsert(
                cursor,
                """
                INSERT INTO norma_versiones (
                    id, documento_id, ref_carga, version_label, numero_version,
                    fecha_inicio_vigencia, fecha_fin_vigencia, es_vigente,
                    texto_consolidado, hash_texto, metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (id) DO UPDATE
                SET documento_id = EXCLUDED.documento_id,
                    ref_carga = EXCLUDED.ref_carga,
                    version_label = EXCLUDED.version_label,
                    numero_version = EXCLUDED.numero_version,
                    fecha_inicio_vigencia = EXCLUDED.fecha_inicio_vigencia,
                    fecha_fin_vigencia = EXCLUDED.fecha_fin_vigencia,
                    es_vigente = EXCLUDED.es_vigente,
                    texto_consolidado = EXCLUDED.texto_consolidado,
                    hash_texto = EXCLUDED.hash_texto,
                    metadata = EXCLUDED.metadata;
                """,
                (
                    version_id,
                    documento_id,
                    version_ref,
                    version.get("version_label"),
                    version.get("numero_version"),
                    version.get("fecha_inicio_vigencia"),
                    version.get("fecha_fin_vigencia"),
                    version.get("es_vigente", True),
                    texto_consolidado,
                    hash_text(texto_consolidado),
                    json.dumps(version.get("metadata", {}), ensure_ascii=False),
                ),
            )
            counters["versiones"] += 1
            if version_ref:
                version_refs[version_ref] = version_id

            for source in version.get("fuentes", []):
                fuente_id = get_fuente_id(cursor, source)
                version_source_id = source.get("id") or stable_uuid(
                    "norma_version_fuente",
                    version_id,
                    source.get("codigo") or source.get("fuente_url"),
                    source.get("ruta_archivo"),
                )
                upsert(
                    cursor,
                    """
                    INSERT INTO norma_version_fuentes (
                        id, version_id, fuente_id, fuente_url, ruta_archivo,
                        checksum_archivo, es_principal, metadata
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (id) DO UPDATE
                    SET version_id = EXCLUDED.version_id,
                        fuente_id = EXCLUDED.fuente_id,
                        fuente_url = EXCLUDED.fuente_url,
                        ruta_archivo = EXCLUDED.ruta_archivo,
                        checksum_archivo = EXCLUDED.checksum_archivo,
                        es_principal = EXCLUDED.es_principal,
                        metadata = EXCLUDED.metadata;
                    """,
                    (
                        version_source_id,
                        version_id,
                        fuente_id,
                        source.get("fuente_url"),
                        source.get("ruta_archivo"),
                        source.get("checksum_archivo"),
                        source.get("es_principal", False),
                        json.dumps(source.get("metadata", {}), ensure_ascii=False),
                    ),
                )

            node_map = process_nodes(
                cursor,
                documento_id=documento_id,
                version_id=version_id,
                nodes=version.get("estructura", []),
                counters=counters,
            )
            node_refs.update(node_map)

            if texto_consolidado:
                synthetic_node = {
                    "tipo_nodo": "documento",
                    "etiqueta": version.get("version_label") or document["nombre_oficial"],
                    "titulo": "Texto consolidado",
                    "texto": texto_consolidado,
                }
                synthetic_ref = stable_uuid("norma_nodo_documento", version_id)
                synthetic_node["ref"] = synthetic_ref
                process_nodes(
                    cursor,
                    documento_id=documento_id,
                    version_id=version_id,
                    nodes=[synthetic_node],
                    counters=counters,
                )

        for relation in document.get("relaciones", []):
            pending_relations.append((documento_id, relation))

    for documento_origen_id, relation in pending_relations:
        destino_id = relation.get("documento_destino_id")
        if not destino_id and relation.get("documento_destino_ref"):
            destino_id = doc_refs.get(relation["documento_destino_ref"])
        if not destino_id:
            continue

        version_origen_id = relation.get("version_origen_id")
        if not version_origen_id and relation.get("version_origen_ref"):
            version_origen_id = version_refs.get(relation["version_origen_ref"])

        version_destino_id = relation.get("version_destino_id")
        if not version_destino_id and relation.get("version_destino_ref"):
            version_destino_id = version_refs.get(relation["version_destino_ref"])

        nodo_origen_id = relation.get("nodo_origen_id")
        if not nodo_origen_id and relation.get("nodo_origen_ref"):
            nodo_origen_id = node_refs.get(relation["nodo_origen_ref"])

        nodo_destino_id = relation.get("nodo_destino_id")
        if not nodo_destino_id and relation.get("nodo_destino_ref"):
            nodo_destino_id = node_refs.get(relation["nodo_destino_ref"])

        relation_id = relation.get("id") or stable_uuid(
            "norma_relacion",
            documento_origen_id,
            destino_id,
            relation["tipo_relacion"],
            relation.get("version_origen_ref"),
            relation.get("version_destino_ref"),
        )
        upsert(
            cursor,
            """
            INSERT INTO norma_relaciones (
                id, documento_origen_id, documento_destino_id, tipo_relacion,
                version_origen_id, version_destino_id, nodo_origen_id, nodo_destino_id,
                fecha_relacion, notas, metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (id) DO UPDATE
            SET documento_origen_id = EXCLUDED.documento_origen_id,
                documento_destino_id = EXCLUDED.documento_destino_id,
                tipo_relacion = EXCLUDED.tipo_relacion,
                version_origen_id = EXCLUDED.version_origen_id,
                version_destino_id = EXCLUDED.version_destino_id,
                nodo_origen_id = EXCLUDED.nodo_origen_id,
                nodo_destino_id = EXCLUDED.nodo_destino_id,
                fecha_relacion = EXCLUDED.fecha_relacion,
                notas = EXCLUDED.notas,
                metadata = EXCLUDED.metadata;
            """,
            (
                relation_id,
                documento_origen_id,
                destino_id,
                relation["tipo_relacion"],
                version_origen_id,
                version_destino_id,
                nodo_origen_id,
                nodo_destino_id,
                relation.get("fecha_relacion"),
                relation.get("notas"),
                json.dumps(relation.get("metadata", {}), ensure_ascii=False),
            ),
        )
        counters["relaciones"] += 1

    return counters


def main():
    parser = argparse.ArgumentParser(description="Carga el corpus normativo de la via lenta desde un JSON.")
    parser.add_argument("--input", required=True, help="Ruta del JSON de entrada")
    args = parser.parse_args()

    input_path = Path(args.input).expanduser().resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"No existe el archivo: {input_path}")

    payload = load_payload(input_path)

    conn = psycopg2.connect(**DB_PARAMS)
    try:
        with conn:
            with conn.cursor() as cursor:
                counters = process_dataset(cursor, payload)
        print("Carga via lenta completada.")
        print(
            "Documentos={documentos}, Versiones={versiones}, Nodos={nodos}, "
            "Fragmentos={fragmentos}, Relaciones={relaciones}".format(**counters)
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
