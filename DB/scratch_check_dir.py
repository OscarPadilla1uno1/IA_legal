import os
SENTENCIAS_DIR = r"D:\Sentencias"
archivos_base = [f for f in os.listdir(SENTENCIAS_DIR) if not 'TipoCertificado' in f and f.endswith('.pdf')]
print(f"Total PDFs found: {len(archivos_base)}")
print(f"First 5: {archivos_base[:5]}")
