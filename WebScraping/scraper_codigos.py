import os
import json
import re
from pathlib import Path
from playwright.sync_api import sync_playwright

OUTPUT_DIR = Path(r"D:\CodigosHN_v2")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST_PATH = OUTPUT_DIR / "codigos_descargados.json"

def get_manifest():
    if not MANIFEST_PATH.exists():
        return []
    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def add_to_manifest(entry):
    manifest = get_manifest()
    manifest.append(entry)
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

def clean_filename(name):
    name = re.sub(r'[\\/*?:"<>|]', "", name)
    name = re.sub(r'\s+', "_", name)
    return name[:150]

def scrape_codigos_tsc(page):
    base_url = "https://www.tsc.gob.hn/biblioteca/"
    print(f"[TSC] Buscando Acervo Jurídico Multisección en: {base_url}")
    try:
        page.goto(base_url, timeout=60000)
    except Exception as e:
        print(f"Error cargando TSC: {e}")
        return

    import urllib.parse
    import urllib.request

    categorias_validas = ['/codigos', '/leyes', '/reglamentos', '/acuerdos', '/tratados', '/varios']
    
    # Extraer los links base de las categorías desde la página de inicio
    try:
        page.wait_for_load_state("domcontentloaded")
    except: pass
    
    urls_a_visitar = []
    # Añadir inicio de categorías
    for cat in categorias_validas:
        urls_a_visitar.append(urllib.parse.urljoin(base_url, f"index.php{cat}"))

    visitadas = set()
    enlaces_detalle = set()

    # Fase 1: Descubrir paginación y páginas de detalle en TODAS las secciones
    while urls_a_visitar:
        url_actual = urls_a_visitar.pop(0)
        if url_actual in visitadas:
            continue
        visitadas.add(url_actual)
        
        print(f"  -> Explorando rama: {url_actual}")
        try:
            page.goto(url_actual, timeout=30000)
            page.wait_for_load_state("domcontentloaded")
            
            links = page.locator("a").all()
            for link in links:
                href = link.get_attribute("href")
                if not href: continue
                full_url = urllib.parse.urljoin(url_actual, href)
                
                # Check if it belongs to any valid category
                is_valid = any(cat in href for cat in categorias_validas)
                if not is_valid: continue
                
                if "?start=" in href and full_url not in visitadas and full_url not in urls_a_visitar:
                    urls_a_visitar.append(full_url)
                elif any(f"{cat}/" in href for cat in categorias_validas) and ".xml" not in href and "feed" not in href and "index.php/tags" not in href:
                    # Es una subpágina de una ley específica
                    enlaces_detalle.add(full_url)
        except Exception as e:
            pass

    print(f"[TSC] {len(enlaces_detalle)} páginas documentales encontradas en todo el sistema. Procediendo a extracción profunda...")

    # Fase 2: Entrar a cada página de detalle y descargar TODOS sus PDFs
    for idx, detail_url in enumerate(list(enlaces_detalle), 1):
        try:
            page.goto(detail_url, timeout=30000)
            page.wait_for_load_state("domcontentloaded")
            
            pdf_links = page.locator("a[href$='.pdf'], a[href*='descargar']").all()
            
            # Use title H2 for primary naming to avoid "Descargar.pdf" conflicts
            titulo_h2 = page.locator("h2[itemprop='name']").first
            doc_titulo_maestro = titulo_h2.inner_text().strip() if titulo_h2.count() > 0 else f"Doc_{idx}"
            
            for pdf_idx, p_link in enumerate(pdf_links, 1):
                href = p_link.get_attribute("href")
                if not href or (".pdf" not in href.lower() and "descargar" not in href.lower()): 
                    continue
                    
                full_pdf_url = urllib.parse.urljoin(detail_url, href)
                
                # Generamos nombre combinando el titulo maestro y posfix para archivos multiples
                text = f"{doc_titulo_maestro}_Anexo_{pdf_idx}" if len(pdf_links) > 1 else doc_titulo_maestro
                filename = clean_filename(text) + ".pdf"
                filepath = OUTPUT_DIR / filename
                
                manifest = get_manifest()
                if any(m['url'] == full_pdf_url for m in manifest) or filepath.exists():
                    continue
                    
                print(f"    ({idx}/{len(enlaces_detalle)}) Descargando: {filename}")
                try:
                    req_pdf = urllib.request.Request(full_pdf_url, headers={'User-Agent': 'Mozilla/5.0'})
                    with urllib.request.urlopen(req_pdf, timeout=30) as pdf_resp:
                        with open(filepath, 'wb') as f:
                            f.write(pdf_resp.read())
                            
                    add_to_manifest({"fuente": "TSC", "titulo": text, "url": full_pdf_url, "archivo": str(filepath)})
                except Exception as err:
                    print(f"    [!] Error HTTP descargando '{text}': {err}")
        except Exception as e:
            pass

def main():
    print("Iniciando Scraper Especializado de Códigos...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, args=['--disable-blink-features=AutomationControlled'])
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        scrape_codigos_tsc(page)
        
        browser.close()
    print(f"Búsqueda de Códigos finalizada. Destino: {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
