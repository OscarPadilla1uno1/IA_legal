import os
import shutil
import random

SOURCE_DIR = r"D:\Sentencias"
TARGET_DIR = r"D:\Sentencias_Validacion_IA"

def main():
    if not os.path.exists(TARGET_DIR):
        os.makedirs(TARGET_DIR)
        
    archivos = os.listdir(SOURCE_DIR)
    
    # Agrupar archivos por "código principal" (la parte antes de pag1 o pag2)
    # Ejemplo: Sentencia_CL_795_00_FRes...
    pares = {}
    for f in archivos:
        if not f.endswith('.pdf'): continue
        if '_pag1.pdf' in f:
            base = f.replace('_pag1.pdf', '')
            if base not in pares: pares[base] = {}
            pares[base]['pag1'] = f
            pares[base]['size'] = os.path.getsize(os.path.join(SOURCE_DIR, f))
        elif 'TipoCertificado' in f:
            # la ficha original solia ser _pag2.pdf, o _pagX, 
            # buscaremos el que hace match quitando el final
            # Wait, `TipoCertificado` files look like:
            # Sentencia_CL_795_00_FRes_..._TipoCertificadoPDF_Magis_..._pag2.pdf
            # Let's just group them by extracting the initial identifier: Sentencia_CL_795_00
            parts = f.split('_FRes_')
            if len(parts) > 1:
                base = parts[0]
                if base not in pares: pares[base] = {}
                pares[base]['ficha'] = f
                
    # Volvemos a recorrer pag1 para asociarlos igual
    # Mejor mapeo: La metadata central es Sentencia_{Materia}_{Numero}_{Año}...
    # Un mapping perfecto:
    pares.clear()
    for f in archivos:
        if f.endswith('.pdf'):
            parts = f.split('_FRes_')
            if len(parts) > 1:
                base_id = parts[0] # Ej: Sentencia_CL_795_00
                if base_id not in pares:
                    pares[base_id] = {'pag1': None, 'ficha': None, 'size': 0}
                
                if 'TipoCertificado' in f:
                    pares[base_id]['ficha'] = f
                elif 'pag1' in f:
                    pares[base_id]['pag1'] = f
                    pares[base_id]['size'] = os.path.getsize(os.path.join(SOURCE_DIR, f))

    # Filtrar solo pares completos
    pares_completos = {k: v for k, v in pares.items() if v['pag1'] and v['ficha']}
    print(f"Total de sentencias completas: {len(pares_completos)}")
    
    k_total = len(pares_completos)
    n_mover = int(k_total * 0.05)
    print(f"Objetivo: Mover {n_mover} sentencias (pares).")
    
    # 1. Agarrar las más largas (mitad del objetivo) -> Dificultad legal
    pares_ordenados = sorted(pares_completos.items(), key=lambda x: x[1]['size'], reverse=True)
    
    a_mover = []
    
    mitad = n_mover // 2
    for i in range(mitad):
        a_mover.append(pares_ordenados[i][0])
        
    # 2. Agarrar aleatoriamente la otra mitad
    resto = pares_ordenados[mitad:]
    random.seed(42)  # Determinismo
    random.shuffle(resto)
    
    for i in range(n_mover - mitad):
        a_mover.append(resto[i][0])
        
    # Mover archivos
    movidos = 0
    for base_id in a_mover:
        archs = pares_completos[base_id]
        shutil.move(os.path.join(SOURCE_DIR, archs['pag1']), os.path.join(TARGET_DIR, archs['pag1']))
        shutil.move(os.path.join(SOURCE_DIR, archs['ficha']), os.path.join(TARGET_DIR, archs['ficha']))
        movidos += 2

    print(f"Éxito: Se movieron {movidos} archivos PDF ({movidos//2} pares) a Validacion_IA.")

if __name__ == "__main__":
    main()
