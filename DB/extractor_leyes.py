import os
import json
import uuid
import datetime
import fitz
import re
from pathlib import Path

INPUT_DIR = Path(r"D:\LeyesHonduras_v2")
OUTPUT_JSON = Path(r"c:\Project\DB\db_leyes_3nf.json")
NAMESPACE = uuid.NAMESPACE_DNS

def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip()

def extract_law_structure(pdf_path):
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        print(f"Error abriendo PDF {pdf_path}: {e}")
        return None

    full_text = ""
    for page in doc:
        full_text += page.get_text("text") + "\n"
    
    # Heurística para obtener el nombre oficial (primera línea o nombre del archivo)
    filename = Path(pdf_path).name
    nombre_oficial = filename.replace("_", " ").replace(".pdf", "")
    
    # 1. Extraer los artículos mediante regex
    # Busca "ARTICULO X.-" o "Artículo X." seguido de texto hasta el próximo artículo.
    article_pattern = re.compile(r'(ART[IÍ]CULO\s+\d+.*?)(?=(?:ART[IÍ]CULO\s+\d+)|$)', re.IGNORECASE | re.DOTALL)
    matches = article_pattern.findall(full_text)
    
    articulos = []
    orden = 1
    for match in matches:
        match_clean = clean_text(match)
        # Extraer el número del artículo
        n_match = re.search(r'ART[IÍ]CULO\s+(\d+)', match_clean, re.IGNORECASE)
        num = n_match.group(1) if n_match else str(orden)
        
        articulos.append({
            "articulo_numero": num,
            "articulo_etiqueta": f"Artículo {num} (Sec #{orden})",
            "texto_oficial": match_clean,
            "orden": orden
        })
        orden += 1

    return {
        "nombre_oficial": nombre_oficial,
        "texto_completo": full_text[:1000],  # guardamos una muestra del inicio
        "articulos": articulos
    }

def main():
    print(f"Procesando normativas en {INPUT_DIR}...")
    
    db_leyes = {
        "legislaciones": [],
        "leyes": [],
        "leyes_versiones": [],
        "articulos_ley": [],
        "nodos_normativos": [],
        "fragmentos_normativos": []
    }
    
    # Creamos una legislacion padre genérica: "Legislación Nacional Hondureña"
    leg_id = str(uuid.uuid5(NAMESPACE, "LEGISLACION_HN"))
    db_leyes["legislaciones"].append({
        "id": leg_id,
        "codigo": "HN-GEN",
        "nombre": "Legislación Nacional Hondureña",
        "descripcion": "Colección general de leyes extraídas automáticamente."
    })
    
    if not INPUT_DIR.exists():
        print(f"El directorio {INPUT_DIR} no existe.")
        return

    for root, dirs, files in os.walk(INPUT_DIR):
        for file in files:
            if not file.lower().endswith(".pdf"):
                continue
                
            file_path = os.path.join(root, file)
            print(f" -> Analizando {file}")
            
            data = extract_law_structure(file_path)
            if not data:
                continue
                
            ley_id = str(uuid.uuid5(NAMESPACE, f"LEY_{data['nombre_oficial']}"))
            version_id = str(uuid.uuid5(NAMESPACE, f"VER_{ley_id}"))
            
            # Registrar Ley
            db_leyes["leyes"].append({
                "id": ley_id,
                "legislacion_id": leg_id,
                "tipo_norma": "ley",
                "nombre_oficial": data["nombre_oficial"],
                "autoridad_emisora": "Congreso Nacional / Poder Ejecutivo"
            })
            
            # Registrar Versión
            db_leyes["leyes_versiones"].append({
                "id": version_id,
                "ley_id": ley_id,
                "version_label": "Versión Original Scrapeada",
                "es_vigente": True
            })
            
            # Registrar Artículos y generar sus fragmentos asociados
            for art in data["articulos"]:
                art_id = str(uuid.uuid5(NAMESPACE, f"ART_{version_id}_{art['orden']}"))
                
                db_leyes["articulos_ley"].append({
                    "id": art_id,
                    "ley_version_id": version_id,
                    "articulo_numero": art["articulo_numero"],
                    "articulo_etiqueta": art["articulo_etiqueta"],
                    "texto_oficial": art["texto_oficial"],
                    "orden": art["orden"]
                })
                
                # Fragmento "ready to embed"
                frag_id = str(uuid.uuid4())
                db_leyes["fragmentos_normativos"].append({
                    "id": frag_id,
                    "ley_id": ley_id,
                    "ley_version_id": version_id,
                    "articulo_id": art_id,
                    "tipo_fragmento": "articulo",
                    "contenido": art["texto_oficial"],
                    "orden": art["orden"]
                })

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(db_leyes, f, indent=4, ensure_ascii=False)
        
    print(f"\n¡Extracción finalizada! Entidades extraídas guardadas en {OUTPUT_JSON}")
    print(f"Total Leyes procesadas: {len(db_leyes['leyes'])}")
    print(f"Total Fragmentos/Artículos listos: {len(db_leyes['articulos_ley'])}")

if __name__ == "__main__":
    main()
