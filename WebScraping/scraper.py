import sys
import re
import os
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

OUTPUT_DIR = Path(r"D:\Sentencias")

def sanitize_filename(text):
    if not text:
        return ""
    clean = re.sub(r'[\s\-/,.]+', '_', text.strip())
    clean = re.sub(r'[\\:*?"<>|]', '', clean)
    return clean

def main():
    global OUTPUT_DIR
    if not OUTPUT_DIR.exists():
        try:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            print(f"Creada la carpeta: {OUTPUT_DIR}")
        except Exception as e:
            print(f"Error creando {OUTPUT_DIR}: {e}")
            print(r"Guardando en el directorio de trabajo (C:\...\Output) en su lugar.")
            OUTPUT_DIR = Path("Output") 
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    max_successful = 10
    success_count = 0
    current_id = 1
    consecutive_failures = 0
    max_consecutive_failures = 20

    print("Iniciando web scraping...")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        while success_count < max_successful and consecutive_failures < max_consecutive_failures:
            url = f"https://sij.poderjudicial.gob.hn/sentences/{current_id}"
            print(f"==========================================")
            print(f"[{current_id}] Visitando {url} ...")
            
            try:
                page.goto(url, wait_until="networkidle")
                
                # Esperamos al elemento de título o al texto de error
                try:
                    page.wait_for_selector("div.col-md-9 h2 strong, p:has-text('Sentencia Consultada no Existe o no es Pública')", timeout=15000)
                except Exception as e:
                    print(f"[{current_id}] Timeout al cargar elementos. Saltando...")
                    consecutive_failures += 1
                    current_id += 1
                    continue
                
                # Verificar si no existe
                not_found = page.locator("p:has-text('Sentencia Consultada no Existe o no es Pública')").count()
                if not_found > 0:
                    print(f"[{current_id}] No existe. Saltando...")
                    consecutive_failures += 1
                    current_id += 1
                    continue
                
                consecutive_failures = 0 
                
                sentencia_raw = page.locator("div.col-md-9 h2 strong").inner_text().strip()
                
                # Función que busca el valor a partir de un texto en negrita
                def get_field_val(label_text):
                    try:
                        # Buscamos en todo el body
                        # En la app, las etiquetas y valores son celdas row col-sm-4 y col-sm-8
                        locators = page.locator(f"xpath=//strong[contains(text(), '{label_text}')]/ancestor::div[contains(@class, 'col-sm-4')]/following-sibling::div[contains(@class, 'col-sm-8')]")
                        if locators.count() > 0:
                            return locators.first.inner_text().strip()
                        return ""
                    except Exception as ex:
                        return ""

                fres_raw = get_field_val('Fecha de resolución')
                if not fres_raw:
                    fres_raw = get_field_val('Fecha resolución')

                magis_raw = get_field_val('Magistrado ponente')
                mate_raw = get_field_val('Materia')
                fsente_raw = get_field_val('Fecha de sentencia recurrida')
                
                # Limpiando variables
                def clean(part_str):
                    return sanitize_filename(part_str)

                sentencia_clean = clean(sentencia_raw)
                fres_clean = clean(fres_raw)
                magis_clean = clean(magis_raw)
                mate_clean = clean(mate_raw)
                fsente_clean = clean(fsente_raw)
                
                base_name = f"Sentencia_{sentencia_clean}"
                if fres_clean: base_name += f"_FRes_{fres_clean}"
                if magis_clean: base_name += f"_Magis_{magis_clean}"
                if mate_clean: base_name += f"_Mate_{mate_clean}"
                if fsente_clean: base_name += f"_FSente_{fsente_clean}"
                
                if len(base_name) > 200:
                    base_name = base_name[:200]
                
                pdf1_name = f"{base_name}.pdf"
                pdf2_name = f"{base_name}_TipoCertificadoPDF.pdf"
                
                pdf1_path = OUTPUT_DIR / pdf1_name
                pdf2_path = OUTPUT_DIR / pdf2_name
                
                # 1. GENERAR PRIMER PDF (Imprimir navegador)
                print(f"[{current_id}] Generando primera versión (Navegador): {pdf1_name}")
                page.pdf(path=str(pdf1_path), format="A4", print_background=True, scale=0.8)
                
                # 2. SEGUNDO PDF (Botón original de la página)
                # OJO: The original button might be an a href or a button. "IMPRIMIR" is a strong keyword.
                btn_imprimir = page.locator("button, a").filter(has_text=re.compile(r"^\s*IMPRIMIR\s*$", re.IGNORECASE))
                if btn_imprimir.count() > 0:
                    try:
                        print(f"[{current_id}] Botón IMPRIMIR existente. Iniciando descarga: {pdf2_name}")
                        with page.expect_download(timeout=45000) as download_info:
                            btn_imprimir.first.click()
                            # El subagent mencionó un popup "Confirmar" de tipo sweetalert con "Si, continuar"
                            btn_confirmar = page.locator("button:has-text('Si, continuar')")
                            try:
                                btn_confirmar.wait_for(state="visible", timeout=3000)
                                btn_confirmar.click()
                            except:
                                # Puede ser que se descargue sin confirmación en algunas páginas
                                pass
                                
                        download = download_info.value
                        download.save_as(str(pdf2_path))
                        print(f"[{current_id}] Descarga exitosa.")
                    except Exception as e:
                        print(f"[{current_id}] Falló la descarga del segundo documento: {e}")
                else:
                    print(f"[{current_id}] Botón IMPRIMIR no encontrado.")
                
                success_count += 1
                print(f"[{current_id}] -> PROGRESO: ({success_count}/{max_successful})")
                
            except Exception as e:
                print(f"[{current_id}] Error general al procesar la sentencia: {e}")
                consecutive_failures += 1
            
            # Pausa para no sobrecargar el servidor
            time.sleep(2)
            current_id += 1

        browser.close()
        print("Finalizado con éxito o por límite de fallas consecutivas.")

if __name__ == "__main__":
    main()
