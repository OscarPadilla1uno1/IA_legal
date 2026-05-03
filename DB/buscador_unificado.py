import sys
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

print("[DEBUG] Script iniciado...")

import os
import time
import argparse
print("[DEBUG] Librerías base cargadas...")

import psycopg2
import torch
print(f"[DEBUG] Torch cargado (Versión: {torch.__version__})")
from sentence_transformers import SentenceTransformer
from modelos_locales import resolver_modelo
print("[DEBUG] Librerías de IA cargadas...")

DB = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432",
}

# ── Carga del modelo ────────────────────────────────────────────────────────

device = "cuda" if torch.cuda.is_available() else "cpu"
modelo_fuente = resolver_modelo("BAAI/bge-m3")
print(f"[IA] Cargando BGE-M3 en {device.upper()} desde: {modelo_fuente}")
model = SentenceTransformer(
    modelo_fuente, device=device, local_files_only=os.path.isdir(modelo_fuente)
)
print("[IA] Modelo listo.\n")


# ── Helpers ─────────────────────────────────────────────────────────────────

def vectorizar(texto: str) -> str:
    vec = model.encode(texto, normalize_embeddings=True)
    return "[" + ",".join(str(x) for x in vec.tolist()) + "]"


def separador(titulo: str, ancho: int = 78):
    print("\n" + "─" * ancho)
    print(f"  {titulo}")
    print("─" * ancho)


# ── Búsqueda de jurisprudencia ───────────────────────────────────────────────

QUERY_JURISPRUDENCIA = """
WITH mejor_frag AS (
    SELECT DISTINCT ON (f.sentencia_id)
        f.sentencia_id,
        f.contenido,
        f.tipo_fragmento,
        1 - (f.embedding_fragmento <=> %s::vector) AS similitud
    FROM fragmentos_texto f
    WHERE f.embedding_fragmento IS NOT NULL
    ORDER BY f.sentencia_id, f.embedding_fragmento <=> %s::vector
)
SELECT
    mf.similitud,
    s.numero_sentencia,
    s.fecha_resolucion,
    s.fallo,
    t.nombre        AS tribunal,
    m.nombre        AS magistrado,
    mat.nombre      AS materia,
    mf.contenido,
    mf.tipo_fragmento
FROM mejor_frag mf
JOIN sentencias s       ON s.id = mf.sentencia_id
LEFT JOIN tribunales t  ON t.id = s.tribunal_id
LEFT JOIN magistrados m ON m.id = s.magistrado_id
LEFT JOIN sentencias_materias sm ON sm.sentencia_id = s.id
LEFT JOIN materias mat  ON mat.id = sm.materia_id
ORDER BY mf.similitud DESC
LIMIT %s;
"""


def buscar_jurisprudencia(vec_str: str, top: int, cursor):
    cursor.execute(QUERY_JURISPRUDENCIA, (vec_str, vec_str, top))
    return cursor.fetchall()


# ── Búsqueda en códigos ──────────────────────────────────────────────────────

QUERY_CODIGOS = """
SELECT
    1 - (a.embedding <=> %s::vector)  AS similitud,
    c.nombre_oficial                   AS codigo,
    a.articulo_etiqueta,
    a.texto_oficial
FROM articulos_codigo a
JOIN codigos_honduras c ON c.id = a.codigo_id
WHERE a.embedding IS NOT NULL
  AND c.nombre_oficial NOT LIKE '[OCR-PENDIENTE]%%'
ORDER BY a.embedding <=> %s::vector
LIMIT %s;
"""


def buscar_codigos(vec_str: str, top: int, cursor):
    cursor.execute(QUERY_CODIGOS, (vec_str, vec_str, top))
    return cursor.fetchall()


# ── Presentación ─────────────────────────────────────────────────────────────

def presentar_resultados(query: str, top: int = 5):
    vec_str = vectorizar(query)

    conn = psycopg2.connect(**DB)
    cur = conn.cursor()

    t0 = time.time()
    jurisprudencia = buscar_jurisprudencia(vec_str, top, cur)
    codigos        = buscar_codigos(vec_str, top, cur)
    t_total = (time.time() - t0) * 1000

    cur.close()
    conn.close()

    print(f'\n{"═"*78}')
    print(f'  BÚSQUEDA LEGAL UNIFICADA — Honduras')
    print(f'  Consulta : "{query}"')
    print(f'  Motor    : BGE-M3 + PostgreSQL pgvector | {device.upper()}')
    print(f'  Tiempo   : {t_total:.1f} ms')
    print(f'{"═"*78}')

    # ── JURISPRUDENCIA ──────────────────────────────────────────────────────
    separador(f"JURISPRUDENCIA  ({len(jurisprudencia)} resultados)")

    if not jurisprudencia:
        print("  (Sin resultados — los fragmentos aún pueden estar vectorizándose)")
    else:
        for i, row in enumerate(jurisprudencia, 1):
            sim, num_sent, fecha, fallo, tribunal, magistrado, materia, contenido, tipo = row
            print(f"\n  [{i}] Similitud: {float(sim)*100:.1f}%")
            print(f"       Sentencia  : {num_sent or 'N/D'}")
            print(f"       Fecha      : {fecha or 'N/D'}")
            print(f"       Tribunal   : {(tribunal or 'N/D')[:120]}")
            print(f"       Magistrado : {magistrado or 'N/D'}")
            print(f"       Materia    : {materia or 'N/D'}")
            print(f"       Fallo      : {(fallo or 'N/D')[:200]}")
            print(f"       Fragmento  ({tipo}):")
            print(f"         {contenido.strip()[:1000]}")

    # ── NORMATIVA (CÓDIGOS) ─────────────────────────────────────────────────
    separador(f"NORMATIVA / CÓDIGOS  ({len(codigos)} resultados)")

    if not codigos:
        print("  (Sin resultados en artículos de códigos)")
    else:
        for i, row in enumerate(codigos, 1):
            sim, codigo, art_etiqueta, texto = row
            print(f"\n  [{i}] Similitud: {float(sim)*100:.1f}%")
            print(f"       Código     : {codigo[:120]}")
            print(f"       Artículo   : {art_etiqueta}")
            print(f"       Texto      :")
            print(f"         {texto.strip()[:1000]}")

    print(f'\n{"═"*78}\n')


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Buscador Legal Unificado — Jurisprudencia + Normativa"
    )
    parser.add_argument("query", nargs="?", default=None,
                        help="Consulta en lenguaje natural")
    parser.add_argument("--top", type=int, default=10,
                        help="Resultados por categoría (default: 10)")
    args = parser.parse_args()

    if args.query:
        presentar_resultados(args.query, args.top)
    else:
        # Modo interactivo
        print("Buscador Legal Unificado — escribe 'salir' para terminar\n")
        while True:
            try:
                consulta = input("Consulta: ").strip()
                if consulta.lower() in ("salir", "exit", "quit", ""):
                    break
                presentar_resultados(consulta, args.top)
            except KeyboardInterrupt:
                break
        print("Hasta luego.")
