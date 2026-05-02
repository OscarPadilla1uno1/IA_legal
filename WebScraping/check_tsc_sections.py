import urllib.request
import re

url = "https://www.tsc.gob.hn/biblioteca/index.php/codigos"
req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
try:
    with urllib.request.urlopen(req, timeout=15) as response:
        html = response.read().decode('utf-8', errors='ignore')
        
    print("Buscando categorías/secciones...")
    # Buscamos otras categorías o links que contengan /biblioteca/index.php/codigos? o similares
    links_secciones = re.findall(r'href="(/biblioteca/index\.php/codigos[^"]*)"', html, re.IGNORECASE)
    links_secciones = list(set(links_secciones))
    print("Links relacionados a códigos:")
    for l in links_secciones[:20]:
        print(f"  - {l}")
        
    # Also check if there are other main library categories
    print("\nBuscando otras ramas legales en el menu principal:")
    ramas = re.findall(r'href="(/biblioteca/index\.php/[^"]+)"', html, re.IGNORECASE)
    ramas = list(set(ramas))
    for r in ramas[:20]:
        if "layout" not in r:
            print(f"  - {r}")

except Exception as e:
    print(e)
