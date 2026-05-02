import urllib.request
import re
import os
from pathlib import Path

OUTPUT_DIR = Path(r"D:\CodigosHN_v2")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def download_codes_simple():
    url = "https://www.tsc.gob.hn/biblioteca/index.php/codigos"
    print(f"Probando conexión HTTP directa a: {url}")
    
    req = urllib.request.Request(
        url, 
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    )
    
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            html = response.read().decode('utf-8', errors='ignore')
            
        # Buscar enlaces PDF
        pdf_links = re.findall(r'href="([^"]+\.pdf)"', html, re.IGNORECASE)
        print(f"Encontrados {len(pdf_links)} enlaces de códigos.")
        
        for link in pdf_links[:5]:  # Descargar los 5 primeros
            full_url = link if link.startswith("http") else f"https://www.tsc.gob.hn{link}"
            
            # Extraer nombre
            name_match = re.search(r'/([^/]+\.pdf)$', link, re.IGNORECASE)
            filename = name_match.group(1) if name_match else "codigo_desconocido.pdf"
            filename = filename.replace("%20", "_")
            filepath = OUTPUT_DIR / filename
            
            print(f"Descargando HTTP: {filename}")
            req_pdf = urllib.request.Request(full_url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req_pdf, timeout=20) as pdf_resp:
                with open(filepath, 'wb') as f:
                    f.write(pdf_resp.read())
                    
        print("Scraping HTTP simple finalizado.")
        
    except Exception as e:
        print(f"Error fatal HTTP: {e}")

if __name__ == "__main__":
    download_codes_simple()
