import urllib.request
import re

url = "https://www.tsc.gob.hn/biblioteca/index.php/codigos"
req = urllib.request.Request(
    url, 
    headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8'
    }
)
try:
    with urllib.request.urlopen(req, timeout=15) as response:
        html = response.read().decode('utf-8', errors='ignore')
    
    pdf_links = re.findall(r'href="([^"]+\.pdf)"', html, re.IGNORECASE)
    print(f"Links encontrados: {pdf_links[:3]}")
    
    # Try downloading the first one properly
    if pdf_links:
        link = pdf_links[0]
        full_url = link if link.startswith("http") else f"https://www.tsc.gob.hn{link}"
        print(f"Descargando: {full_url}")
        
        req_pdf = urllib.request.Request(
            full_url, 
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'Accept': 'application/pdf,*/*;q=0.8',
                'Referer': url
            }
        )
        with urllib.request.urlopen(req_pdf, timeout=20) as pdf_resp:
            content = pdf_resp.read()
            print(f"Exito. Tamaño: {len(content)} bytes")
except Exception as e:
    print(f"Error: {e}")
