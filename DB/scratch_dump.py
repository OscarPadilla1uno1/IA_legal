import fitz
t = ""
try:
    doc = fitz.open(r'D:\Sentencias\Sentencia_CL_795_00_FRes_01_09_2000_Magis_Teodolinda_Pineda_Cardona_Mate_Derecho_Laboral_FSente_29_03_2000_pag1.pdf')
    for p in doc:
        t += p.get_text('text') + '\n'
except Exception as e:
    t = str(e)
with open('dump.txt', 'w', encoding='utf-8') as f:
    f.write(t)
