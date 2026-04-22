import re
import sys
import unicodedata

import psycopg2

from db_config import connect_db

sys.stdout.reconfigure(encoding="utf-8")

LAW_KEYWORDS = (
    "constitucion",
    "codigo",
    "ley",
    "convencion",
    "declaracion",
    "reglamento",
    "estatuto",
    "tratado",
)


def normalize_text(value):
    if value is None:
        return None
    value = unicodedata.normalize("NFKD", str(value))
    value = "".join(char for char in value if not unicodedata.combining(char))
    value = re.sub(r"\s+", " ", value).strip().lower()
    return value or None


def looks_like_law_name(value):
    norm = normalize_text(value)
    if not norm:
        return False
    return any(keyword in norm for keyword in LAW_KEYWORDS)


def extract_article_hint(*values):
    article_like = []
    for value in values:
        norm = normalize_text(value)
        if not norm:
            continue
        if re.fullmatch(r"(articulo\s*)?\d+[a-z]?", norm):
            article_like.append(re.sub(r"articulo\s*", "", norm))
        elif re.fullmatch(r"\d+[a-z]?", norm):
            article_like.append(norm)
    if article_like:
        return article_like[-1]
    return None


def infer_reference(nombre_ley, articulo, sub_indice):
    fields = [nombre_ley, articulo, sub_indice]
    name_parts = [field.strip() for field in fields if looks_like_law_name(field)]
    article_hint = extract_article_hint(nombre_ley, articulo, sub_indice)

    if not name_parts:
        for first, second in ((nombre_ley, articulo), (articulo, sub_indice)):
            joined = " ".join(part.strip() for part in (first, second) if part)
            if looks_like_law_name(joined):
                name_parts = [joined]
                break

    alias = normalize_text(" ".join(name_parts)) if name_parts else None
    return alias, article_hint


def fetch_aliases(cursor):
    cursor.execute(
        """
        SELECT la.ley_id, la.alias_normalizado, l.nombre_oficial
        FROM leyes_alias la
        JOIN leyes l ON l.id = la.ley_id
        """
    )
    mapping = {}
    for ley_id, alias_normalizado, nombre_oficial in cursor.fetchall():
        mapping.setdefault(alias_normalizado, []).append((ley_id, nombre_oficial))
    return mapping


def match_law(alias_map, alias_hint):
    if not alias_hint:
        return None, 0.0
    exact = alias_map.get(alias_hint)
    if exact and len(exact) == 1:
        return exact[0][0], 0.99

    partials = []
    for alias, rows in alias_map.items():
        if alias_hint in alias or alias in alias_hint:
            partials.extend(rows)
    unique = {ley_id for ley_id, _ in partials}
    if len(unique) == 1:
        return partials[0][0], 0.75
    return None, 0.0


def match_article(cursor, ley_id, article_hint):
    if not ley_id or not article_hint:
        return None, 0.0
    cursor.execute(
        """
        SELECT al.id
        FROM articulos_ley al
        JOIN leyes_versiones lv ON lv.id = al.ley_version_id
        WHERE lv.ley_id = %s
          AND (
            LOWER(al.articulo_numero) = LOWER(%s)
            OR LOWER(al.articulo_etiqueta) = LOWER(%s)
            OR LOWER(al.articulo_etiqueta) = LOWER(CONCAT('Artículo ', %s))
          )
        ORDER BY lv.es_vigente DESC, al.orden ASC
        LIMIT 1
        """,
        (ley_id, article_hint, article_hint, article_hint),
    )
    row = cursor.fetchone()
    if row:
        return row[0], 0.98
    return None, 0.0


def main():
    conn = connect_db()
    cursor = conn.cursor()

    try:
        alias_map = fetch_aliases(cursor)
        cursor.execute("SELECT id, nombre_ley, articulo, sub_indice FROM legislacion")
        refs = cursor.fetchall()

        matched_law = 0
        matched_article = 0
        pending = 0

        for ref_id, nombre_ley, articulo, sub_indice in refs:
            alias_hint, article_hint = infer_reference(nombre_ley, articulo, sub_indice)
            ley_id, confidence = match_law(alias_map, alias_hint)
            articulo_id, article_confidence = match_article(cursor, ley_id, article_hint)

            status = "pendiente"
            if articulo_id:
                status = "articulo_encontrado"
                matched_article += 1
            elif ley_id:
                status = "ley_encontrada"
                matched_law += 1
            else:
                pending += 1

            cursor.execute(
                """
                INSERT INTO mapeo_legislacion_canonica (
                    legislacion_id, ley_id, articulo_ley_id, alias_detectado,
                    confianza, estado_match, notas
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (legislacion_id) DO UPDATE
                SET ley_id = EXCLUDED.ley_id,
                    articulo_ley_id = EXCLUDED.articulo_ley_id,
                    alias_detectado = EXCLUDED.alias_detectado,
                    confianza = EXCLUDED.confianza,
                    estado_match = EXCLUDED.estado_match,
                    notas = EXCLUDED.notas;
                """,
                (
                    ref_id,
                    ley_id,
                    articulo_id,
                    alias_hint,
                    max(confidence, article_confidence),
                    status,
                    f"article_hint={article_hint}" if article_hint else None,
                ),
            )

            if articulo_id:
                cursor.execute(
                    """
                    INSERT INTO sentencias_articulos_ley (
                        sentencia_id, articulo_ley_id, fuente_extraccion, alias_detectado, confianza
                    )
                    SELECT sl.sentencia_id, %s, 'reconciliacion_automatica', %s, %s
                    FROM sentencias_legislacion sl
                    WHERE sl.legislacion_id = %s
                    ON CONFLICT (sentencia_id, articulo_ley_id) DO UPDATE
                    SET alias_detectado = EXCLUDED.alias_detectado,
                        confianza = GREATEST(sentencias_articulos_ley.confianza, EXCLUDED.confianza);
                    """,
                    (articulo_id, alias_hint, article_confidence, ref_id),
                )

        conn.commit()
        print("Reconciliación completada.")
        print(f"leyes encontradas: {matched_law}")
        print(f"articulos encontrados: {matched_article}")
        print(f"pendientes: {pending}")
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
