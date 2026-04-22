import json
import psycopg2
from psycopg2.extras import execute_values

import os
from dotenv import load_dotenv

load_dotenv()

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432"
}

def load_json(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def insert_bulk(cursor, table_name, list_data, columns):
    if not list_data:
        return
        
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING;"
    
    values = []
    for item in list_data:
        row = []
        for col in columns:
            val = item.get(col)
            if isinstance(val, str):
                val = val.replace('\x00', '')
            row.append(val)
        values.append(tuple(row))
        
    execute_values(cursor, query, values)
    print(f"[{table_name}] Inyectados {len(list_data)} registros.")

def main():
    print("Iniciando inyector SQL PostgreSQL MÁSIVO...")
    # Buscamos el JSON en el mismo directorio que el script
    base_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(base_dir, 'db_dump_3nf_FINAL.json')
    data = load_json(json_path)
    
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    try:
        # ---- 1. Entidades Independientes ----
        insert_bulk(cursor, "tipos_proceso", data.get("tipos_proceso", []), ["id", "nombre", "subtipo"])
        insert_bulk(cursor, "magistrados", data.get("magistrados", []), ["id", "nombre", "cargo"])
        insert_bulk(cursor, "tribunales", data.get("tribunales", []), ["id", "nombre"])
        insert_bulk(cursor, "personas_entidades", data.get("personas_entidades", []), ["id", "nombre", "tipo", "rol_habitual"])
        insert_bulk(cursor, "materias", data.get("materias", []), ["id", "nombre", "materia_padre_id"])
        insert_bulk(cursor, "legislacion", data.get("legislacion", []), ["id", "nombre_ley", "articulo", "sub_indice"])
        
        # ---- 2. Tabla Central: Sentencias ----
        sentencias = data.get("sentencias", [])
        # Tratamiento especial porque los foreign keys pueden estar ahí o no (y debemos garantizar que encajen perfectos en Postgres)
        # Campos: id, numero_sentencia, fecha_resolucion, fecha_sentencia_recurrida, fallo, anonimizada, texto_integro, vigencia_jurisprudencial, jerarquia_jurisprudencial, tiene_novedades, embedding, tipo_proceso_id, magistrado_id, tribunal_id, recurrente_id, recurrido_id
        cols_sentencias = [
            "id", "numero_sentencia", "fecha_resolucion", "fecha_sentencia_recurrida", "fallo", 
            "anonimizada", "texto_integro", "vigencia_jurisprudencial", "jerarquia_jurisprudencial", 
            "tiene_novedades", "tipo_proceso_id", "magistrado_id", "tribunal_id", "recurrente_id", "recurrido_id"
        ]
        # Nota: no insertamos 'embedding' todavia
        insert_bulk(cursor, "sentencias", sentencias, cols_sentencias)
        
        # ---- 3. Tablas Pivote N:M ----
        insert_bulk(cursor, "sentencias_materias", data.get("sentencias_materias", []), ["sentencia_id", "materia_id"])
        insert_bulk(cursor, "sentencias_legislacion", data.get("sentencias_legislacion", []), ["sentencia_id", "legislacion_id"])
        
        # ---- 4. Modelos RAG Hijos ----
        insert_bulk(cursor, "tesauro", data.get("tesauro", []), ["id", "sentencia_id", "categoria", "subcategoria", "problema_juridico", "respuesta_juridica"])
        cols_fragmentos = ["id", "sentencia_id", "tipo_fragmento", "contenido", "orden"]
        insert_bulk(cursor, "fragmentos_texto", data.get("fragmentos_texto", []), cols_fragmentos)
        insert_bulk(cursor, "documentos_pdf", data.get("documentos_pdf", []), ["id", "sentencia_id", "ruta_archivo"])

        conn.commit()
        print("¡Inyección exitosa completa! Todos los datos fueron guardados usando integridad ACID.")
        
    except Exception as e:
        conn.rollback()
        print(f"Error fatal durante inyección, Rollback ejecutado: {e}")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
