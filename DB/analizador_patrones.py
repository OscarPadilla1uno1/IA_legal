import os
import fitz
import re
from collections import defaultdict
import concurrent.futures
import time

SENTENCIAS_DIR = r"D:\Sentencias"

def analyze_document(filename):
    path = os.path.join(SENTENCIAS_DIR, filename)
    doc_headers = defaultdict(int)
    has_tesauro = False
    has_certificacion = False
    missing_fields = []
    
    try:
        doc = fitz.open(path)
        full_text = ""
        for page in doc:
            full_text += page.get_text("text") + "\n"
        doc.close()
        
        lines = [line.strip() for line in full_text.split('\n') if line.strip()]
        
        for line in lines:
            if len(line) < 3 or len(line) > 55: continue
            if line.endswith('.') or line.endswith(','): continue
            if re.match(r'^[\d/\-]+$', line): continue
            if re.match(r'(Página|Centro Electrónico|De Información|FICHA)', line, re.IGNORECASE): continue
            
            norm_line = line.capitalize() 
            doc_headers[norm_line] += 1
            
            up_line = line.upper()
            if "TESAURO" in up_line: has_tesauro = True
            if "CERTIFICACIÓN" in up_line: has_certificacion = True

        if not has_tesauro: missing_fields.append("SIN_TESAURO")
        if not has_certificacion: missing_fields.append("SIN_CERTIFICACION_BASE")

        return True, filename, dict(doc_headers), missing_fields
        
    except Exception as e:
        return False, filename, str(e), []

def main():
    archivos = [f for f in os.listdir(SENTENCIAS_DIR) if f.endswith('.pdf') and 'TipoCertificado' in f]
    print(f"Iniciando escaneo PARALELIZADO del 100% de la carpera: {len(archivos)} fichas...")
    
    start_t = time.time()
    header_freq = defaultdict(int)
    header_multiples = defaultdict(int)
    anomaly_counts = defaultdict(int)
    errors = []
    
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = executor.map(analyze_document, archivos)
        
        for success, filename, headers_or_error, missing in results:
            if not success:
                errors.append((filename, headers_or_error))
                continue
                
            for h, count in headers_or_error.items():
                header_freq[h] += 1
                if count > 1:
                    header_multiples[h] += 1
                    
            for anomaly in missing:
                anomaly_counts[anomaly] += 1
                
    t_elapsed = time.time() - start_t
    print(f"\nEscaneo terminado en {t_elapsed:.2f} segundos.")
    
    min_occurence = max(1, int(len(archivos) * 0.01)) # Minimo 1% de aparición para considerarlo un header estable
    
    patrones_comunes = {k: v for k, v in header_freq.items() if v >= min_occurence}
    
    print("\n--- POSIBLES SUBTÍTULOS / CAMPOS DETECTADOS ---")
    for k, v in sorted(patrones_comunes.items(), key=lambda item: item[1], reverse=True)[:50]:
        multi_str = f" (Aparece MÚLTIPLES veces: {header_multiples[k]} docs)" if header_multiples[k] > 0 else ""
        print(f"[{v}/{len(archivos)} docs] -> {k}{multi_str}")
        
    print("\n--- ANOMALÍAS ESTRUCTURALES ---")
    for an, count in anomaly_counts.items():
         print(f"{an}: Presente en {count} documentos ({(count/len(archivos))*100:.2f}%)")
         
    if len(errors) > 0:
        print(f"\nDocumentos Ilegibles o Corruptos: {len(errors)}")

if __name__ == "__main__":
    main()
