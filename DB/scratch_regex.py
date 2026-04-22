import re
import json

def parse_pdf2(text):
    partes = {
        "tesauros": [],
        "considerandos": "",
        "por_tanto": "",
        "texto_limpio_completo": ""
    }
    partes["texto_limpio_completo"] = re.sub(r'\s+', ' ', text).strip()
    
    # Separar todo lo que está antes de la SENTENCIA (El Fallo en sí)
    match_sentencia = re.search(r'(Sentencia\s*CERTIFICACI[OÓ]N|CERTIFICACI[OÓ]N|Certificaci[oó]n)', text, re.IGNORECASE)
    
    if match_sentencia:
        metadata_block = text[:match_sentencia.start()]
        fallo_block = text[match_sentencia.start():]
    else:
        metadata_block = text
        fallo_block = ""
        
    # Extraer tesauros del bloque de metadatos (Pueden ser múltiples)
    tesauro_blocks = re.split(r'\nTesauro\s*\n', metadata_block)[1:]
    
    for tb in tesauro_blocks:
        tes_obj = {
            "tesauro_categoria": None,
            "tesauro_problema": None,
            "tesauro_respuesta": None,
            "consideraciones_sala": None,
            "legislaciones": []
        }
        
        # Categoria: primera linea despues del split (usualmente algo como Derecho Procesal Laboral)
        if "\n" in tb:
            tes_obj["tesauro_categoria"] = tb.split("\n", 1)[0].strip()
            
        prob_match = re.search(r'¿(.+?\?)', tb)
        if prob_match:
            tes_obj["tesauro_problema"] = "¿" + prob_match.group(1).strip()
            
        resp_match = re.search(r'Respuesta al problema jurídico\s*\n(.+?)(?=\nConsideraciones de la sala|\nLegislación aplicada|\nTesauro|\Z)', tb, re.DOTALL | re.IGNORECASE)
        if resp_match:
            tes_obj["tesauro_respuesta"] = re.sub(r'\s+', ' ', resp_match.group(1)).strip()
            
        cons_match = re.search(r'Consideraciones de la sala\s*\n(.+?)(?=\nLegislación aplicada|\nTesauro|\Z)', tb, re.DOTALL | re.IGNORECASE)
        if cons_match:
            tes_obj["consideraciones_sala"] = re.sub(r'\s+', ' ', cons_match.group(1)).strip()
            
        # Extraer filas de "Legislación aplicada"
        leg_match = re.search(r'Legislación aplicada\s*\nArtículo\s*\nSub Indice\s*\n(.+?)(?=\nJerarquía|\nTesauro|\Z)', tb, re.DOTALL | re.IGNORECASE)
        if leg_match:
            filas_raw = [r.strip() for r in leg_match.group(1).split("\n") if r.strip()]
            # Podrian venir combinadas si no hay split perfecto. Intentemos iterar de 3 en 3 líneas si asume "Ley", "Art", "Subindice"
            # O mejor, en dbg_pdf_utf8.txt dice:
            # 70: Código del Trabajo
            # 71: 769
            # 72: Numeral 5 literal b
            i = 0
            while i + 2 < len(filas_raw):
                ley = filas_raw[i]
                art = filas_raw[i+1]
                sub = filas_raw[i+2]
                tes_obj["legislaciones"].append({"ley": ley, "articulo": art, "sub_indice": sub})
                i += 3
                
        partes["tesauros"].append(tes_obj)
        
    start_cons = fallo_block.upper().find("CONSIDERANDO")
    end_cons = fallo_block.upper().find("POR TANTO")
    if end_cons == -1: end_cons = fallo_block.upper().find("FALLA")
    
    if start_cons != -1 and end_cons != -1:
        partes["considerandos"] = re.sub(r'\s+', ' ', fallo_block[start_cons:end_cons]).strip()
        partes["por_tanto"] = re.sub(r'\s+', ' ', fallo_block[end_cons:]).strip()
    elif start_cons != -1:
        partes["considerandos"] = re.sub(r'\s+', ' ', fallo_block[start_cons:]).strip()
        
    return partes

text = open('c:/Project/DB/dbg_pdf_utf8.txt', 'r', encoding='utf-8').read()
out = parse_pdf2(text)
print(json.dumps(out, indent=2, ensure_ascii=False))
