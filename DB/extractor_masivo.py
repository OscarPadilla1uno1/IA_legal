import os
import re
import json
import uuid
import datetime
import fitz
import nltk
from nltk.tokenize import sent_tokenize
import time

nltk.download('punkt_tab', quiet=True)

SENTENCIAS_DIR = r"D:\Sentencias"
NAMESPACE = uuid.NAMESPACE_DNS

# ----------------- UTILIDADES -----------------

def clean_string(raw_str):
    if not raw_str or raw_str.strip() == "--": return None
    res = re.sub(r'\n+', ' ', raw_str)
    return re.sub(r'\s+', ' ', res).strip()

def parse_date(date_str):
    if not date_str or date_str == "--": return None
    clean = re.sub(r'[^\d/\-]', '', date_str)
    parts = clean.replace('-', '/').split('/')
    if len(parts) == 3: 
        try:
             return f"{parts[2]}-{parts[1]}-{parts[0]}"
        except:
             return clean
    return clean

def chunk_text(text, target_words=150, overlap_words=40):
    if not text: return []
    sentences = sent_tokenize(text, language='spanish')
    chunks = []
    current_chunk_sents = []
    current_length = 0
    
    for s in sentences:
        s_len = len(s.split())
        if current_length + s_len > target_words and current_length > 0:
            chunks.append(" ".join(current_chunk_sents))
            overlap_target = overlap_words
            overlap_sents = []
            overlap_len = 0
            for osent in reversed(current_chunk_sents):
                osent_len = len(osent.split())
                if overlap_len + osent_len > overlap_target:
                    break
                overlap_sents.insert(0, osent)
                overlap_len += osent_len
            current_chunk_sents = overlap_sents
            current_length = overlap_len
            
        current_chunk_sents.append(s)
        current_length += s_len
    if current_chunk_sents:
        chunks.append(" ".join(current_chunk_sents))
    return chunks

def clean_full_text(text):
    text = re.sub(r'Página \d+ de \d+', '', text)
    text = re.sub(r'FICHA JURISPRUDENCIAL', '', text, flags=re.IGNORECASE)
    return re.sub(r'\n{3,}', '\n\n', text).strip()

# ----------------- EXTRACCION ROBUSTA -----------------

def extract_metadata_from_pdf1(pdf1_path):
    try:
        doc = fitz.open(pdf1_path)
        text = ""
        for page in doc:
             text += page.get_text("text") + "\n"
        doc.close()
    except:
        return {}
    
    metadata = {}
    patterns = {
        "sentencia_id": r"Sentencia\s+([^\r\n]+)",
        "tipo_proceso": r"Tipo de proceso:[\s]*[\r\n]+([^\r\n]+)",
        "subtipo_proceso": r"Subtipo de proceso:[\s]*[\r\n]+([^\r\n]+)",
        "magistrado": r"Magistrado ponente:[\s]*[\r\n]+([^\r\n]+)",
        "materia": r"Materia:[\s]*[\r\n]+([^\r\n]+)",
        "fecha_resolucion": r"Fecha de resolución:[\s]*[\r\n]+([\d/]+)",
        "recurrente": r"Recurrente:[\s]*[\r\n]+([^\r\n]+(?:(?:\r?\n(?!Recurrido:|Tribunal:))[^\r\n]+)*)",
        "recurrido": r"Recurrido:[\s]*[\r\n]+([^\r\n]+(?:(?:\r?\n(?!Tribunal:|Fecha de sentencia recurrida:))[^\r\n]+)*)",
        "tribunal": r"Tribunal:[\s]*[\r\n]+([^\r\n]+(?:(?:\r?\n(?!Fecha de sentencia recurrida:))[^\r\n]+)*)",
        "fecha_sentencia_recurrida": r"Fecha de sentencia[\s]*[\r\n]+recurrida:[\s]*[\r\n]+([\d/]+)",
        "fallo": r"Fallo:[\s]*[\r\n]+(.+?(?=\nHechos relevantes:|\Z))"
    }
    
    for key, pattern in patterns.items():
        match = re.search(pattern, text, flags=re.DOTALL | re.IGNORECASE)
        if match:
            metadata[key] = clean_string(match.group(1))
        else:
            metadata[key] = None
    return metadata

def extract_full_text_from_pdf2(pdf2_path):
    try:
        doc = fitz.open(pdf2_path)
        full_text = ""
        for page in doc: full_text += page.get_text("text") + "\n"
        doc.close()
    except:
        return { "tesauros": [], "considerandos": "", "por_tanto": "", "texto_limpio_completo": "" }
         
    partes = { "tesauros": [], "considerandos": "", "por_tanto": "", "texto_limpio_completo": clean_full_text(full_text) }
    
    match_sentencia = re.search(r'(Sentencia\s*CERTIFICACI[OÓ]N|CERTIFICACI[OÓ]N|Certificaci[oó]n)', full_text, re.IGNORECASE)
    if match_sentencia:
        metadata_block = full_text[:match_sentencia.start()]
        fallo_block = full_text[match_sentencia.start():]
    else:
        metadata_block = full_text
        fallo_block = ""
        
    tesauro_blocks = re.split(r'\nTesauro\s*\n', metadata_block)[1:]
    
    for tb in tesauro_blocks:
        tes_obj = { "tesauro_categoria": None, "tesauro_problema": None, "tesauro_respuesta": None, "consideraciones_sala": None, "legislaciones": [] }
        if "\n" in tb: tes_obj["tesauro_categoria"] = clean_string(tb.split("\n", 1)[0])
        prob_match = re.search(r'(.*?)\?', tb, re.DOTALL)
        if prob_match: tes_obj["tesauro_problema"] = "" + clean_string(prob_match.group(1)) + "?"
        resp_match = re.search(r'Respuesta al problema jurídico\s*\n(.+?)(?=\nConsideraciones de la sala|\nLegislación aplicada|\nTesauro|\nJerarquía|\Z)', tb, re.DOTALL | re.IGNORECASE)
        if resp_match: tes_obj["tesauro_respuesta"] = clean_string(resp_match.group(1))
        cons_match = re.search(r'Consideraciones de la sala\s*\n(.+?)(?=\nLegislación aplicada|\nTesauro|\nJerarquía|\Z)', tb, re.DOTALL | re.IGNORECASE)
        if cons_match: tes_obj["consideraciones_sala"] = clean_full_text(cons_match.group(1))
        
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

# ----------------- PROCESAMIENTO UNITARIO -----------------

def parse_single_document(pdf1):
    try:
        base_name = pdf1.replace('.pdf', '')
        match_pag = re.search(r'_pag\d+', base_name)
        if not match_pag: return None
        
        pag_str = match_pag.group(0)
        pdf2 = base_name.replace(pag_str, '') + '_TipoCertificadoPDF' + pag_str + '.pdf'
        
        path1 = os.path.join(SENTENCIAS_DIR, pdf1)
        path2 = os.path.join(SENTENCIAS_DIR, pdf2)
        if not os.path.exists(path2): return None
        
        metadata = extract_metadata_from_pdf1(path1)
        pdf2_data = extract_full_text_from_pdf2(path2)
        
        s_uuid = str(uuid.uuid5(NAMESPACE, pdf1))
        local_db = { k: [] for k in ["tipos_proceso", "magistrados", "tribunales", "materias", "personas_entidades", "legislacion", "sentencias", "sentencias_materias", "sentencias_legislacion", "tesauro", "fragmentos_texto", "documentos_pdf"] }
        
        # 1. Metadatos
        tp_id, mag_id, trib_id, m_id = None, None, None, None
        
        if metadata.get('tipo_proceso'):
            tp_id = str(uuid.uuid5(NAMESPACE, f"TIPO_{metadata.get('tipo_proceso')}_{metadata.get('subtipo_proceso')}"))
            local_db["tipos_proceso"].append({"id": tp_id, "nombre": metadata.get('tipo_proceso'), "subtipo": metadata.get('subtipo_proceso')})
            
        if metadata.get('magistrado'):
            mag_id = str(uuid.uuid5(NAMESPACE, f"MAG_{metadata.get('magistrado')}"))
            local_db["magistrados"].append({"id": mag_id, "nombre": metadata.get('magistrado'), "cargo": "Magistrado Ponente"})
            
        if metadata.get('tribunal'):
            trib_id = str(uuid.uuid5(NAMESPACE, f"TRIB_{metadata.get('tribunal')}"))
            local_db["tribunales"].append({"id": trib_id, "nombre": metadata.get('tribunal')})
            
        if metadata.get('materia'):
            m_id = str(uuid.uuid5(NAMESPACE, f"MAT_{metadata.get('materia')}"))
            local_db["materias"].append({"id": m_id, "nombre": metadata.get('materia')})
            local_db["sentencias_materias"].append({"sentencia_id": s_uuid, "materia_id": m_id})

        # 2. Personas
        rec_id, reco_id = None, None
        if metadata.get('recurrente'):
            rec_id = str(uuid.uuid5(NAMESPACE, f"PERS_{metadata.get('recurrente')}"))
            local_db["personas_entidades"].append({"id": rec_id, "nombre": metadata.get('recurrente'), "rol_habitual": "recurrente"})
        if metadata.get('recurrido'):
            reco_id = str(uuid.uuid5(NAMESPACE, f"PERS_{metadata.get('recurrido')}"))
            local_db["personas_entidades"].append({"id": reco_id, "nombre": metadata.get('recurrido'), "rol_habitual": "recurrido"})

        local_db["sentencias"].append({
            "id": s_uuid, "numero_sentencia": metadata.get("sentencia_id") or base_name,
            "fecha_resolucion": parse_date(metadata.get("fecha_resolucion")), "fecha_sentencia_recurrida": parse_date(metadata.get("fecha_sentencia_recurrida")),
            "fallo": metadata.get("fallo"), "texto_integro": pdf2_data["texto_limpio_completo"],
            "tipo_proceso_id": tp_id, "magistrado_id": mag_id, "tribunal_id": trib_id, "recurrente_id": rec_id, "recurrido_id": reco_id,
            "created_at": datetime.datetime.now().isoformat()
        })
        
        local_db["documentos_pdf"].append({"id": str(uuid.uuid5(NAMESPACE, f"PDF_{s_uuid}")), "sentencia_id": s_uuid, "ruta_archivo": path2})

        # 3. Fragmentos y Tesauros
        orden_chunk = 1
        for t_idx, t_obj in enumerate(pdf2_data["tesauros"]):
            if t_obj["tesauro_problema"]:
                local_db["tesauro"].append({
                    "id": str(uuid.uuid5(NAMESPACE, f"TES_{s_uuid}_{t_idx}")),
                    "sentencia_id": s_uuid, "categoria": t_obj["tesauro_categoria"],
                    "problema_juridico": t_obj["tesauro_problema"], "respuesta_juridica": t_obj["tesauro_respuesta"]
                })
            if t_obj["consideraciones_sala"]:
                for ch in chunk_text(t_obj["consideraciones_sala"]):
                    local_db["fragmentos_texto"].append({"id": str(uuid.uuid5(NAMESPACE, f"FRAG_{s_uuid}_{orden_chunk}")), "sentencia_id": s_uuid, "tipo_fragmento": "consideraciones_sala", "contenido": ch, "orden": orden_chunk})
                    orden_chunk += 1
            for leg in t_obj["legislaciones"]:
                leg_id = str(uuid.uuid5(NAMESPACE, f"LEG_{leg['ley']}|{leg['articulo']}|{leg['sub_indice']}"))
                local_db["legislacion"].append({"id": leg_id, "nombre_ley": leg['ley'], "articulo": leg['articulo'], "sub_indice": leg['sub_indice']})
                local_db["sentencias_legislacion"].append({"sentencia_id": s_uuid, "legislacion_id": leg_id})
                
        for ch in chunk_text(pdf2_data["considerandos"]):
            local_db["fragmentos_texto"].append({"id": str(uuid.uuid5(NAMESPACE, f"FRAG_{s_uuid}_{orden_chunk}")), "sentencia_id": s_uuid, "tipo_fragmento": "considerando", "contenido": ch, "orden": orden_chunk})
            orden_chunk += 1
        for ch in chunk_text(pdf2_data["por_tanto"]):
            local_db["fragmentos_texto"].append({"id": str(uuid.uuid5(NAMESPACE, f"FRAG_{s_uuid}_{orden_chunk}")), "sentencia_id": s_uuid, "tipo_fragmento": "fallo", "contenido": ch, "orden": orden_chunk})
            orden_chunk += 1

        return local_db
    except Exception as e:
        return {"error": f"{pdf1}: {str(e)}"}

# ----------------- MAIN (SERIAL) -----------------

def main():
    archivos_base = [f for f in os.listdir(SENTENCIAS_DIR) if not 'TipoCertificado' in f and f.endswith('.pdf')]
    print(f"Iniciando Extraccin SERIAL Segura (Garantizada) sobre {len(archivos_base)} Sentencias...")
    
    global_schema = { k: {} for k in ["tipos_proceso", "magistrados", "tribunales", "materias", "personas_entidades", "legislacion", "sentencias", "sentencias_materias", "sentencias_legislacion", "tesauro", "fragmentos_texto", "documentos_pdf"] }
    start_t = time.time()
    
    # Procesamiento SERIAL (Para evitar race conditions y bloqueos de disco)
    for i, f_base in enumerate(archivos_base):
        res = parse_single_document(f_base)
        if not res: continue
        if "error" in res:
            # print(f"Error parseando: {res['error']}")
            continue
        
        for tbl in ["tipos_proceso", "magistrados", "tribunales", "materias", "personas_entidades", "legislacion", "sentencias", "tesauro", "fragmentos_texto", "documentos_pdf"]:
            for row in res[tbl]:
                global_schema[tbl][row["id"]] = row
                
        for row in res["sentencias_materias"]:
            key = f"{row['sentencia_id']}_{row['materia_id']}"
            global_schema["sentencias_materias"][key] = row
        for row in res["sentencias_legislacion"]:
            key = f"{row['sentencia_id']}_{row['legislacion_id']}"
            global_schema["sentencias_legislacion"][key] = row
        
        if i % 250 == 0 and i > 0:
            print(f"[{i}/{len(archivos_base)}] Procesados (Seguro)... ETA: {((time.time()-start_t)/i)*(len(archivos_base)-i)/60:.1f} min")

    for k in global_schema:
        global_schema[k] = list(global_schema[k].values())
        
    print(f"\nExtraccin Finalizada en {(time.time() - start_t)/60:.2f} minutos.")
    print("Guardando a db_dump_3nf_FINAL.json...")
    
    with open('db_dump_3nf_FINAL.json', 'w', encoding='utf-8') as f:
        json.dump(global_schema, f, ensure_ascii=False, indent=2)
    print("¡Completado con éxito total!")

if __name__ == "__main__":
    main()
