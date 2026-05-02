import sys
sys.stdout.reconfigure(encoding="utf-8")
"""
Renombrador FINAL — Itera manifest, abre el PDF desde disco,
extrae el nombre real y actualiza la BD buscando por created_at
(el manifest y los inserts en BD se hicieron en orden correlativo).
"""

import json
import re
import os
import psycopg2
import fitz

MANIFEST_PATH = r"D:\CodigosHN_v2\codigos_descargados.json"

DB = {"dbname":"legal_ia","user":"root","password":"rootpassword","host":"localhost","port":"5432"}

PATRONES_NOMBRE = [
    r'((?:LEY|CODIGO|CÓDIGO|REGLAMENTO|DECRETO(?:\s+LEY)?|ACUERDO|CONVENIO|TRATADO|NORMA|DIRECTIVA|RESOLUCION|RESOLUCIÓN)[\s\w\d\-\(\)\"\'ÁÉÍÓÚÜÑ]{10,180}?)(?=\n|\.\s|ARTÍCULO|CAPITULO|TITULO)',
    r'(DECRETO\s+(?:N[°Oº\.]\s*|NÚMERO\s+|NUMERO\s+)[\d\-\w]+[^\n]{0,120})',
    r'(ACUERDO\s+(?:EJECUTIVO|MINISTERIAL|N[°Oº\.]\s*)[\d\-\w]+[^\n]{0,100})',
]

RUIDO = [
    r'EMPRESA NACIONAL DE ARTES GR',
    r'DIARIO OFICIAL DE LA REPUBLICA',
    r'REPÚBLICA\s+DE\s+HONDURAS\s*[-]\s*TEGUCIGALPA',
    r'REPUBLICA\s+DE\s+HONDURAS',
    r'^\d+\s+La Gaceta',
    r'^La Gaceta',
    r'Sección\s+[AB]\s+Acuerdos',
    r'AÑO\s+C[DLMXVI]+',
    r'La primera imprenta llegó',
    r'^\d[\d\s]*$',
]


def es_ruido(t):
    return any(re.search(p, t, re.IGNORECASE) for p in RUIDO)


def extraer_nombre(pdf_path):
    try:
        doc = fitz.open(pdf_path)
        texto = ""
        for i in range(min(len(doc), 5)):
            texto += doc[i].get_text("text") + "\n"
        doc.close()
        texto = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', texto).strip()
        if not texto:
            return None, True

        for patron in PATRONES_NOMBRE:
            m = re.search(patron, texto, re.IGNORECASE | re.DOTALL)
            if m:
                c = re.sub(r'\s+', ' ', m.group(1)).strip()
                if len(c) >= 12 and not es_ruido(c):
                    return c[:200], False

        lineas = [l.strip() for l in texto.splitlines() if len(l.strip()) > 8]
        for linea in lineas[:30]:
            if not es_ruido(linea):
                return linea[:200], False

        return "[SIN-NOMBRE]", False
    except Exception:
        return None, False


def main():
    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    conn = psycopg2.connect(**DB)
    cur = conn.cursor()

    # Traer todas las filas ordenadas por created_at (mismo orden que los inserts originales)
    cur.execute("SELECT id, nombre_oficial, metadata FROM codigos_honduras ORDER BY created_at ASC;")
    filas_bd = cur.fetchall()

    if len(filas_bd) != len(manifest):
        print(f"ADVERTENCIA: BD tiene {len(filas_bd)} filas, manifest tiene {len(manifest)} entradas")

    total = min(len(manifest), len(filas_bd))
    actualizados = 0
    imagenes = 0
    sin_archivo = 0
    sin_cambio = 0

    print(f"Procesando {total} documentos...")
    print("-" * 60)

    for idx in range(total):
        entry = manifest[idx]
        doc_id, nombre_actual, metadata = filas_bd[idx]

        titulo_scraper = entry["titulo"]
        ruta = entry["archivo"]

        if not os.path.exists(ruta):
            sin_archivo += 1
            continue

        nombre_extraido, es_imagen = extraer_nombre(ruta)
        nuevo_metadata = dict(metadata or {})
        nuevo_metadata["titulo_scraper"] = titulo_scraper

        if es_imagen:
            nuevo_nombre = f"[OCR-PENDIENTE] {titulo_scraper[:120]}"
            imagenes += 1
            print(f"[{idx+1}/{total}] IMAGEN: {titulo_scraper[:55]}")
        elif nombre_extraido and nombre_extraido != "[SIN-NOMBRE]" and nombre_extraido != nombre_actual:
            nuevo_nombre = nombre_extraido
            actualizados += 1
            print(f"[{idx+1}/{total}] OK: '{titulo_scraper[:30]}' -> '{nombre_extraido[:55]}'")
        else:
            cur.execute(
                "UPDATE codigos_honduras SET metadata = %s WHERE id = %s;",
                (json.dumps(nuevo_metadata, ensure_ascii=False), str(doc_id))
            )
            sin_cambio += 1
            continue

        cur.execute(
            "UPDATE codigos_honduras SET nombre_oficial = %s, metadata = %s WHERE id = %s;",
            (nuevo_nombre, json.dumps(nuevo_metadata, ensure_ascii=False), str(doc_id))
        )

    conn.commit()
    cur.close()
    conn.close()

    print("\n" + "=" * 60)
    print(f"  Actualizados con nombre real  : {actualizados}")
    print(f"  Marcados como imagen/OCR      : {imagenes}")
    print(f"  Sin cambio (ya correcto)      : {sin_cambio}")
    print(f"  Sin archivo en disco          : {sin_archivo}")
    print("=" * 60)


if __name__ == "__main__":
    main()
