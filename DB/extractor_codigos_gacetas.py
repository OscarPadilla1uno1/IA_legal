import os
import fitz
import re
import psycopg2
from pathlib import Path
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432"
}

def clean_text(t):
    t = t.replace("\x00", "")
    return re.sub(r'\s+', ' ', t).strip()

class ExtractorInyector:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_PARAMS)
        self.cursor = self.conn.cursor()

    def procesar_codigos(self, directory):
        print("\n=== Procesando Códigos ===")
        dir_path = Path(directory)
        if not dir_path.exists():
            print(f"Directorio no encontrado: {directory}")
            return
            
        for file in dir_path.glob("*.pdf"):
            print(f"Procesando Código: {file.name}")
            doc = fitz.open(file)
            texto = ""
            for page in doc: texto += page.get_text("text") + "\n"
            
            # Registrar Código Padre
            nombre_oficial = file.stem.replace("_", " ")
            self.cursor.execute(
                "INSERT INTO codigos_honduras (nombre_oficial) VALUES (%s) RETURNING id",
                (nombre_oficial,)
            )
            codigo_id = self.cursor.fetchone()[0]

            # Analizar jerarquía usando Expresiones Regulares secuenciales
            # Buscamos Capítulos y recolectamos sus IDs para asociar Artículos
            capitulos_raw = re.split(r'(?i)(CAP[IÍ]TULO\s+[IVXLCDM]+\b)', texto)
            
            # Si el split funcionó, el indice 0 es basura/preambulo, indice 1 es CAPITULO, indice 2 es contenido, indice 3 es otro CAPITULO, etc.
            if len(capitulos_raw) > 1:
                idx = 1
                orden_cap = 1
                orden_art = 1
                while idx < len(capitulos_raw):
                    cap_etiqueta = clean_text(capitulos_raw[idx])
                    cap_contenido = capitulos_raw[idx+1]
                    
                    self.cursor.execute(
                        """INSERT INTO capitulos_codigo (codigo_id, capitulo_etiqueta, texto_aglomerado, orden)
                           VALUES (%s, %s, %s, %s) RETURNING id""",
                        (codigo_id, cap_etiqueta, clean_text(cap_contenido)[:5000], orden_cap)
                    )
                    cap_id = self.cursor.fetchone()[0]
                    
                    # Dentro del contenido de este capitulo, buscar articulos
                    articles = re.split(r'(?i)(ART[IÍ]CULO\s+\d+\.-?\s)', cap_contenido)
                    if len(articles) > 1:
                        a_idx = 1
                        while a_idx < len(articles):
                            art_etiq = clean_text(articles[a_idx])
                            art_text = clean_text(articles[a_idx+1])
                            
                            self.cursor.execute(
                                """INSERT INTO articulos_codigo (capitulo_id, codigo_id, articulo_numero, articulo_etiqueta, texto_oficial, orden)
                                   VALUES (%s, %s, %s, %s, %s, %s)""",
                                (cap_id, codigo_id, art_etiq.split()[-1], art_etiq, art_text, orden_art)
                            )
                            orden_art += 1
                            a_idx += 2
                            
                    orden_cap += 1
                    idx += 2
                    
            self.conn.commit()
            print(f" -> Código {nombre_oficial} inyectado con éxito.")

    def procesar_gacetas(self, directory):
        print("\n=== Procesando La Gaceta ===")
        dir_path = Path(directory)
        if not dir_path.exists():
            print(f"Directorio no encontrado: {directory}")
            return
            
        for file in dir_path.glob("*.pdf"):
            print(f"Procesando Gaceta: {file.name}")
            doc = fitz.open(file)
            texto = ""
            for page in doc: texto += page.get_text("text") + "\n"
            
            # Buscar edición en el texto
            match_edicion = re.search(r'(?i)EDICI[OÓ]N\s+(\d+)', texto)
            numero_edicion = match_edicion.group(1) if match_edicion else "Desconocida"
            
            self.cursor.execute(
                """INSERT INTO gacetas_oficiales (numero_edicion, texto_integro) 
                   VALUES (%s, %s)""",
                (numero_edicion, clean_text(texto))
            )
            
            self.conn.commit()
            print(f" -> Gaceta {numero_edicion} inyectada con éxito.")

    def cerrar(self):
        self.cursor.close()
        self.conn.close()

if __name__ == "__main__":
    extractor = ExtractorInyector()
    extractor.procesar_codigos(r"D:\CodigosHN_v2")
    extractor.procesar_gacetas(r"D:\GacetasHN")
    extractor.cerrar()
