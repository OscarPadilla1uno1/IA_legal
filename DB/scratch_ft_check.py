import sentence_transformers, torch, sys
sys.stdout.reconfigure(encoding='utf-8')
print('sentence-transformers:', sentence_transformers.__version__)
print('torch:', torch.__version__)
print('CUDA:', torch.cuda.is_available())
if torch.cuda.is_available():
    props = torch.cuda.get_device_properties(0)
    vram_total = props.total_memory / 1024**3
    vram_free  = (props.total_memory - torch.cuda.memory_allocated()) / 1024**3
    print(f'GPU: {props.name}')
    print(f'VRAM total: {vram_total:.1f} GB')
    print(f'VRAM libre: {vram_free:.1f} GB')

import psycopg2
DB = {'dbname':'legal_ia','user':'root','password':'rootpassword','host':'localhost','port':'5432'}
conn = psycopg2.connect(**DB)
cur = conn.cursor()
cur.execute("""
    SELECT s.fallo_macro, COUNT(DISTINCT ft.sentencia_id) as n_sent, COUNT(ft.id) as n_frag
    FROM fragmentos_texto ft
    JOIN sentencias s ON s.id = ft.sentencia_id
    WHERE s.fallo_macro IS NOT NULL AND s.fallo_macro != 'DESCONOCIDO'
    GROUP BY s.fallo_macro ORDER BY n_frag DESC;
""")
print('\nFragmentos disponibles por clase:')
for r in cur.fetchall():
    print(f'  {r[0]:<18} {r[1]:>6} sentencias  {r[2]:>8} fragmentos')
cur.close()
conn.close()
