import os
import time
import json
import re
from pathlib import Path
from playwright.sync_api import sync_playwright

OUTPUT_DIR = Path(r"D:\LeyesHonduras_v2")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST_PATH = OUTPUT_DIR / "leyes_descargadas.json"

def init_manifest():
    if not MANIFEST_PATH.exists():
        with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
            json.dump([], f)

def get_manifest():
    with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def add_to_manifest(entry):
    manifest = get_manifest()
    manifest.append(entry)
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

def clean_filename(name):
    # Remplazar caracteres no válidos para nombres de archivo
    name = re.sub(r'[\\/*?:"<>|]', "", name)
    name = re.sub(r'\s+', "_", name)
    return name[:150]

def scrape_tsc_laws(page):
    url = "https://www.tsc.gob.hn/biblioteca/index.php/leyes"
    print(f"Iniciando scraping en TSC: {url}")
    try:
        page.goto(url, timeout=60000)
    except Exception as e:
        print(f"Error cargando TSC: {e}")
        return

    page_num = 1
    max_pages_demo = 2 # Limitamos a 2 paginas para demostracion/prueba inicial
    
    while page_num <= max_pages_demo:
        print(f"  Procesando página {page_num}...")
        try:
            page.wait_for_selector("a[href$='.pdf'], a[href*='descargar']", timeout=30000)
        except:
            print("  No se encontraron PDFs o la página tardó demasiado.")
            break
            
        links = page.locator("a").all()
        for link in links:
            try:
                href = link.get_attribute("href")
                if href and (".pdf" in href.lower() or "download" in href.lower() or "descargar" in href.lower()):
                    text = link.inner_text().strip()
                    if not text:
                        text = "Documento_TSC_Desconocido"
                    
                    full_url = href if href.startswith("http") else f"https://www.tsc.gob.hn{href}"
                    filename = clean_filename(text) + ".pdf"
                    filepath = OUTPUT_DIR / filename
                    
                    manifest = get_manifest()
                    if any(m['url'] == full_url for m in manifest) or filepath.exists():
                        continue
                    
                    print(f"    -> Descargando: {filename}")
                    try:
                        # Para descargar en playwright synchronous, abrimos una nueva pestaña y guardamos
                        with page.expect_download(timeout=30000) as download_info:
                            link.click()
                        download = download_info.value
                        download.save_as(str(filepath))
                        
                        add_to_manifest({
                            "fuente": "TSC",
                            "titulo": text,
                            "url": full_url,
                            "archivo": str(filepath)
                        })
                    except Exception as err:
                        print(f"    [!] Error descargando '{text}': {err}")
            except Exception as e:
                pass
                
        # Intentar ir a la siguiente página
        try:
            next_btn = page.locator("a[title='Siguiente'], a[title='Next'], a:has-text('Siguiente')").first
            if next_btn and next_btn.is_visible():
                next_btn.click()
                page.wait_for_load_state("networkidle")
                page_num += 1
            else:
                break
        except:
            break

def main():
    init_manifest()
    print("Iniciando Scraper Robusto de Leyes Hondureñas (Desde Cero)...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        scrape_tsc_laws(page)
        # Podriamos agregar CEDIJ aca tambien
        # scrape_cedij_laws(page)
        
        browser.close()
    print(f"Scraping concluido. Documentos guardados en {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
