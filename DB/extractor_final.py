import os
import json
import fitz  # PyMuPDF
import re
import uuid
import datetime
import nltk
from nltk.tokenize import sent_tokenize

SENTENCIAS_DIR = r"D:\Sentencias"

# ----------------- INICIALIZACIÓN DEL ESQUEMA 3FN -----------------
db_schema = {
    "sentencias": [],
    "tipos_proceso": [],
    "magistrados": [],
    "tribunales": [],
    "personas_entidades": [],
    "materias": [],
    "sentencias_materias": [],
    "legislacion": [],
    "sentencias_legislacion": [],
    "tesauro": [],
    "fragmentos_texto": [],
    "documentos_pdf": []
}

# Diccionarios de búsqueda rápida (HashMaps) para garantizar Unicidad (1FN/2FN)
map_tipos_proceso = {}
map_magistrados = {}
map_tribunales = {}
map_personas = {}
map_materias = {}
map_legislacion = {}
# ----------------- UTILIDADES -----------------

def clean_string(raw_str):
    if not raw_str or raw_str.strip() == "--":
        return None
    res = re.sub(r'\n+', ' ', raw_str)
    return re.sub(r'\s+', ' ', res).strip()

def parse_date(date_str):
    if not date_str or date_str == "--": return None
    # Asume formato hondureño DD/MM/YYYY o DD-MM-YYYY
    clean = re.sub(r'[^\d/\-]', '', date_str)
    # Aquí podríamos convertir obj_date = datetime.strptime() per dejaremos string YYYY-MM-DD para compatibilidad Postgres
    parts = clean.replace('-', '/').split('/')
    if len(parts) == 3:
        return f"{parts[2]}-{parts[1]}-{parts[0]}"
    return clean

def get_or_create_entity(id_map, target_list, key_str, creation_func):
    """
    Busca si la entidad (ej. un Juez específico) ya fue ingresada a la BD global.
    Si no existe, genera un nuevo UUID, lo agrega a la tabla SQL simulada, y retorna su ID.
    """
    key_clean = clean_string(key_str)
    if not key_clean:
        return None
        
    key_upper = key_clean.upper()
    if key_upper not in id_map:
        new_id = str(uuid.uuid4())
        id_map[key_upper] = new_id
        target_list.append(creation_func(new_id, key_clean))
        
    return id_map[key_upper]

# ----------------- EXTRACCIÓN Y CHUNKING -----------------

def chunk_text(text, target_words=150, overlap_words=40):
    if not text:
        return []
        
    sentences = sent_tokenize(text, language='spanish')
    chunks = []
    
    current_chunk_sents = []
    current_length = 0
    
    for sentence in sentences:
        words_count = len(sentence.split())
        
        if current_length + words_count > target_words and current_length > 0:
            chunks.append(" ".join(current_chunk_sents))
            
            overlap_sents = []
            overlap_length = 0
            for s in reversed(current_chunk_sents):
                s_len = len(s.split())
                if overlap_length + s_len <= overlap_words or overlap_length == 0:
                    overlap_sents.insert(0, s)
                    overlap_length += s_len
                else:
                    break
                    
            current_chunk_sents = overlap_sents
            current_length = overlap_length
            
        current_chunk_sents.append(sentence)
        current_length += words_count
        
    if current_chunk_sents:
        chunks.append(" ".join(current_chunk_sents))
        
    return chunks

def extract_metadata_from_pdf1(pdf1_path):
    doc = fitz.open(pdf1_path)
    text = ""
    for page in doc:
         text += page.get_text("text") + "\n"
    
    metadata = {}
    patterns = {
        "sentencia_id": r"Sentencia\s+([A-Z0-9\-]+)",
        "tipo_proceso": r"Tipo de proceso:\s*\n([^\n]+)",
        "subtipo_proceso": r"Subtipo de proceso:\s*\n([^\n]+)",
        "magistrado": r"Magistrado ponente:\s*\n([^\n]+)",
        "materia": r"Materia:\s*\n([^\n]+)",
        "fecha_resolucion": r"Fecha de resolución:\s*\n([\d/]+)",
        "fecha_sentencia_recurrida": r"Fecha de sentencia\s*recurrida:\s*\n([\d/]+)",
        "recurrente": r"Recurrente:\s*\n([^\n]+(?:(?:\n(?!Recurrido:|Tribunal:))[^\n]+)*)", # Multiplínea atrapador
        "recurrido": r"Recurrido:\s*\n([^\n]+(?:(?:\n(?!Tribunal:|Fecha))[^\n]+)*)",
        "tribunal": r"Tribunal:\s*\n([^\n]+(?:(?:\n(?!Fecha))[^\n]+)*)",
        "fallo": r"Fallo:\s*\n(.+?(?=\nHechos relevantes:|\Z))"
    }
    
    for key, pattern in patterns.items():
        match = re.search(pattern, text, flags=re.DOTALL)
        if match:
            metadata[key] = clean_string(match.group(1))
        else:
            metadata[key] = None
            
    return metadata

def clean_full_text(text):
    text = re.sub(r'Página \d+ de \d+', '', text)
    text = re.sub(r'FICHA JURISPRUDENCIAL', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\n+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def extract_full_text_from_pdf2(pdf2_path):
    doc = fitz.open(pdf2_path)
    full_text = ""
    for page in doc:
         full_text += page.get_text("text") + "\n"
         
    partes = {
        "tesauros": [],
        "considerandos": "",
        "por_tanto": "",
        "texto_limpio_completo": clean_full_text(full_text)
    }
    
    # Separar todo lo que está antes de la SENTENCIA (El Fallo en sí)
    match_sentencia = re.search(r'(Sentencia\s*CERTIFICACI[OÓ]N|CERTIFICACI[OÓ]N|Certificaci[oó]n)', full_text, re.IGNORECASE)
    
    if match_sentencia:
        metadata_block = full_text[:match_sentencia.start()]
        fallo_block = full_text[match_sentencia.start():]
    else:
        metadata_block = full_text
        fallo_block = ""
        
    tesauro_blocks = re.split(r'\nTesauro\s*\n', metadata_block)[1:]
    
    for tb in tesauro_blocks:
        tes_obj = {
            "tesauro_categoria": None,
            "tesauro_problema": None,
            "tesauro_respuesta": None,
            "consideraciones_sala": None,
            "legislaciones": []
        }
        
        if "\n" in tb:
            tes_obj["tesauro_categoria"] = clean_string(tb.split("\n", 1)[0])
            
        prob_match = re.search(r'¿(.*?)\?', tb, re.DOTALL)
        if prob_match:
            tes_obj["tesauro_problema"] = "¿" + clean_string(prob_match.group(1)) + "?"
            
        resp_match = re.search(r'Respuesta al problema jurídico\s*\n(.+?)(?=\nConsideraciones de la sala|\nLegislación aplicada|\nTesauro|\nJerarquía|\Z)', tb, re.DOTALL | re.IGNORECASE)
        if resp_match:
            tes_obj["tesauro_respuesta"] = clean_string(resp_match.group(1))
            
        cons_match = re.search(r'Consideraciones de la sala\s*\n(.+?)(?=\nLegislación aplicada|\nTesauro|\nJerarquía|\Z)', tb, re.DOTALL | re.IGNORECASE)
        if cons_match:
            tes_obj["consideraciones_sala"] = clean_full_text(cons_match.group(1))
            
        leg_match = re.search(r'Legislación aplicada\s*\nArtículo\s*\nSub Indice\s*\n(.+?)(?=\nJerarquía|\nTesauro|\Z)', tb, re.DOTALL | re.IGNORECASE)
        if leg_match:
            filas_raw = [r.strip() for r in leg_match.group(1).split("\n") if r.strip()]
            i = 0
            while i + 2 < len(filas_raw):
                tes_obj["legislaciones"].append({"ley": clean_string(filas_raw[i]), "articulo": clean_string(filas_raw[i+1]), "sub_indice": clean_string(filas_raw[i+2])})
                i += 3
                
        partes["tesauros"].append(tes_obj)
        
    start_cons = fallo_block.upper().find("CONSIDERANDO")
    end_cons = fallo_block.upper().find("POR TANTO")
    if end_cons == -1: end_cons = fallo_block.upper().find("FALLA")
    
    if start_cons != -1 and end_cons != -1:
        partes["considerandos"] = clean_full_text(fallo_block[start_cons:end_cons])
        partes["por_tanto"] = clean_full_text(fallo_block[end_cons:])
    elif start_cons != -1:
        partes["considerandos"] = clean_full_text(fallo_block[start_cons:])
        
    return partes


# ----------------- MOTOR PRINCIPAL ORQUESTADOR -----------------

def main():
    archivos = os.listdir(SENTENCIAS_DIR)
    
    print("Iniciando Extracción Racional a Modelo 3FN...")
    
    archivos_base = [f for f in archivos if not 'TipoCertificado' in f and f.endswith('.pdf')]
    # Selección de prueba, puedes remover [:2] luego para todas
    muestra = sorted(archivos_base, key=lambda x: int(re.search(r'_pag(\d+)\.pdf', x).group(1)))[:2]
    
    for pdf1 in muestra:
        print(f"-> Analizando documento principal: {pdf1}")
        base_name = pdf1.replace('.pdf', '')
        pdf2 = base_name.replace(re.search(r'_pag\d+', base_name).group(0), '') + '_TipoCertificadoPDF' + re.search(r'_pag\d+', base_name).group(0) + '.pdf'
        
        path1 = os.path.join(SENTENCIAS_DIR, pdf1)
        path2 = os.path.join(SENTENCIAS_DIR, pdf2)
        
        if not os.path.exists(path2):
            print(f"Omitiendo {base_name}: Falta Ficha Certificada")
            continue
            
        try:
            metadata = extract_metadata_from_pdf1(path1)
            pdf2_data = extract_full_text_from_pdf2(path2)
            
            # --- 1. Sentencia Principal (Generamos su ID) ---
            s_uuid = str(uuid.uuid4())
            
            # --- 2. Poblar Tipos de Proceso Normalizado ---
            tp_key = f"{metadata.get('tipo_proceso')}|{metadata.get('subtipo_proceso')}"
            tp_id = get_or_create_entity(map_tipos_proceso, db_schema["tipos_proceso"], tp_key, 
                lambda uid, val: {"id": uid, "nombre": metadata.get('tipo_proceso'), "subtipo": metadata.get('subtipo_proceso')}
            )
            
            # --- 3. Magistrados Normalizado ---
            mag_id = get_or_create_entity(map_magistrados, db_schema["magistrados"], metadata.get('magistrado'),
                lambda uid, val: {"id": uid, "nombre": val, "cargo": "Magistrado Ponente", "activo": True}
            )
            
            # --- 4. Tribunales Normalizado ---
            trib_id = get_or_create_entity(map_tribunales, db_schema["tribunales"], metadata.get('tribunal'),
                lambda uid, val: {"id": uid, "nombre": val}
            )
            
            # --- 5. Materias y Tabla Pivote ---
            m_id = get_or_create_entity(map_materias, db_schema["materias"], metadata.get('materia'),
                lambda uid, val: {"id": uid, "materia_padre_id": None, "nombre": val}
            )
            if m_id:
                db_schema["sentencias_materias"].append({"sentencia_id": s_uuid, "materia_id": m_id})
                
            # --- 6. Documento PDF Físico ---
            db_schema["documentos_pdf"].append({
                "id": str(uuid.uuid4()),
                "sentencia_id": s_uuid,
                "ruta_archivo": path2
            })
            
            # --- 7. Crear el Registro de Sentencia Maestro ---
            db_schema["sentencias"].append({
                "id": s_uuid,
                "numero_sentencia": metadata.get("sentencia_id"),
                "fecha_resolucion": parse_date(metadata.get("fecha_resolucion")),
                "fecha_sentencia_recurrida": parse_date(metadata.get("fecha_sentencia_recurrida")),
                "fallo": metadata.get("fallo"),
                "anonimizada": False, 
                "texto_integro": pdf2_data["texto_limpio_completo"],
                "vigencia_jurisprudencial": "Vigente",
                "jerarquia_jurisprudencial": "Reiterativa",
                "tiene_novedades": False,
                "embedding": None, # Target para IA futuro
                "created_at": datetime.datetime.now().isoformat(),
                "tipo_proceso_id": tp_id,
                "magistrado_id": mag_id,
                "tribunal_id": trib_id
            })
            
            # --- 8. Personas / Entidades (Recurrentes y Recurridos) ---
            def parse_personas(texto, tipo, rol):
                if not texto: return
                # Asumiremos que pueden separarse por comas en un futuro, o tomar la cadena limpia
                p_id = get_or_create_entity(map_personas, db_schema["personas_entidades"], texto,
                    lambda uid, val: {"id": uid, "nombre": val, "tipo": "institución_pública" if "Tribunal" in val or "Corte" in val else "persona_natural", "rol_habitual": rol}
                )
                # OJO: La instrucción decía que las llaves van en sentencia, lo adaptamos si quisiéramos actualizar sentencia.
                # Como ya insertamos la sentencia arriba, buscaremos actualizar su diccionario:
                for s in db_schema["sentencias"]:
                    if s["id"] == s_uuid:
                        s[f"{rol}_id"] = p_id
            
            parse_personas(metadata.get('recurrente'), 'persona_natural', 'recurrente')
            parse_personas(metadata.get('recurrido'), 'empresa', 'recurrido')

            # --- 9. Extraer Fragmentos de Texto (RAG Core) y Tesauros 1:N ---
            orden_chunk = 1
            
            for t_obj in pdf2_data["tesauros"]:
                # Guardar Tesauro
                if t_obj["tesauro_problema"] and t_obj["tesauro_respuesta"]:
                    db_schema["tesauro"].append({
                        "id": str(uuid.uuid4()),
                        "sentencia_id": s_uuid,
                        "categoria": t_obj["tesauro_categoria"],
                        "subcategoria": None,
                        "problema_juridico": t_obj["tesauro_problema"],
                        "respuesta_juridica": t_obj["tesauro_respuesta"]
                    })
                    
                # Guardar Consideraciones de la Sala como fragmentos_texto
                if t_obj["consideraciones_sala"]:
                    chunks_cons_sala = chunk_text(t_obj["consideraciones_sala"])
                    for ch in chunks_cons_sala:
                        db_schema["fragmentos_texto"].append({
                            "id": str(uuid.uuid4()),
                            "sentencia_id": s_uuid,
                            "tipo_fragmento": "consideraciones_sala",
                            "contenido": ch,
                            "orden": orden_chunk,
                            "embedding_fragmento": None
                        })
                        orden_chunk += 1
                        
                # 9.5 Procesar Legislación 1:N
                for leg in t_obj["legislaciones"]:
                    leg_key = f"{leg['ley']}|{leg['articulo']}|{leg['sub_indice']}"
                    leg_id = get_or_create_entity(
                        map_legislacion, 
                        db_schema["legislacion"], 
                        leg_key,
                        lambda uid, val: {"id": uid, "nombre_ley": leg['ley'], "articulo": leg['articulo'], "sub_indice": leg['sub_indice']}
                    )
                    if leg_id:
                        db_schema["sentencias_legislacion"].append({
                            "sentencia_id": s_uuid,
                            "legislacion_id": leg_id
                        })
            
            # --- 10. Considerandos Principales y Fallo ---
            chunks_considerandos = chunk_text(pdf2_data["considerandos"])
            for ch in chunks_considerandos:
                db_schema["fragmentos_texto"].append({
                    "id": str(uuid.uuid4()),
                    "sentencia_id": s_uuid,
                    "tipo_fragmento": "considerando",
                    "contenido": ch,
                    "orden": orden_chunk,
                    "embedding_fragmento": None
                })
                orden_chunk += 1
                
            chunks_fallo = chunk_text(pdf2_data["por_tanto"])
            for ch in chunks_fallo:
                db_schema["fragmentos_texto"].append({
                    "id": str(uuid.uuid4()),
                    "sentencia_id": s_uuid,
                    "tipo_fragmento": "fallo",
                    "contenido": ch,
                    "orden": orden_chunk,
                    "embedding_fragmento": None
                })
                orden_chunk += 1
                
        except Exception as e:
            print(f"Error procesando {pdf1}: {e}")
            
    # Volcar la super-base normalizada relacional
    out_file = r"c:\Project\DB\db_dump_3nf.json"
    with open(out_file, 'w', encoding='utf-8') as f:
        json.dump(db_schema, f, indent=4, ensure_ascii=False)
        
    print(f"\n¡Dump Completado! Guardado en: {out_file}")

if __name__ == "__main__":
    main()
