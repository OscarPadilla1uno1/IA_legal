import subprocess
import sys
import time

# Forzar UTF-8 para problemas de codificación de consola en Windows
sys.stdout.reconfigure(encoding="utf-8")

def ejecutar_script(nombre, ruta, entorno=None):
    print(f"\n=============================================")
    print(f"-> INICIANDO: {nombre}")
    print(f"=============================================")
    
    cmd = [sys.executable if not entorno else entorno, ruta]
    
    start = time.time()
    # Usamos Popen para streaming en tiempo real
    process = subprocess.Popen(
        cmd, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT,
        text=True,
        encoding='utf-8',
        errors='replace'
    )
    
    for line in iter(process.stdout.readline, ''):
        print(line, end='')
        
    process.stdout.close()
    return_code = process.wait()
    elapsed = time.time() - start
    
    if return_code == 0:
        print(f"\n[OK] EXITOSO: {nombre} (Completado en {elapsed:.2f}s)")
    else:
        print(f"\n[FAIL] ERROR: {nombre} falló con código {return_code}")
    
    return return_code == 0

def main():
    print("Iniciando Orquestador Maestro de Capa 2 (Miles de Documentos)...")
    
    # 1. Scraping Masivo (Llegarán todos los PDFs a sus carpetas)
    ejecutar_script("Scraper de Códigos (TSC/CEDIJ)", "./scraper_codigos.py")
    ejecutar_script("Scraper de La Gaceta (ENAG/IAIP)", "./scraper_gaceta.py")
    
    # 2. Extracción Estructurada y Subida a DB
    print("\n[PAUSA] Esperando estabilización de archivos en disco...")
    time.sleep(3)
    exito_db = ejecutar_script("Extractor e Inyector DB (PostgreSQL)", "../DB/extractor_codigos_gacetas.py")
    
    # 3. Vectorización BGE-M3 (Usa el VENV por dependencias de Torch/SentenceTransformers)
    if exito_db:
        # Asumiendo que el VENV gbe está en c:\Project\DB\venv_bge
        ruta_venv = r"C:\Project\DB\venv_bge\Scripts\python.exe"
        ejecutar_script("Vectorizador Nativo BGE-M3", "../DB/vectorizador_codigos_gacetas.py", entorno=ruta_venv)
    else:
        print("Cancelando Vectorización debido a fallo en la inyección de la DB.")

if __name__ == "__main__":
    main()
