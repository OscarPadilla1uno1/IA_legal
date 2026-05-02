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

def clean_null_bytes(val):
    if isinstance(val, str):
        return val.replace('\x00', '')
    return val

def insert_bulk(cursor, table_name, list_data, columns):
    if not list_data:
        return
        
    cols = ", ".join(columns)
    query = f"INSERT INTO {table_name} ({cols}) VALUES %s ON CONFLICT DO NOTHING;"
    
    values = []
    for item in list_data:
        row = tuple(clean_null_bytes(item.get(col)) for col in columns)
        values.append(row)
        
    execute_values(cursor, query, values)
    print(f"[{table_name}] Inyectados {len(list_data)} registros.")

def main():
    print("Iniciando inyector SQL para Normativa Estructurada (Leyes)...")
    json_path = 'c:/Project/DB/db_leyes_3nf.json'
    
    if not os.path.exists(json_path):
        print(f"Error: No se encuentra {json_path}")
        return
        
    data = load_json(json_path)
    
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    try:
        # ---- 1. Tablas Maestras ----
        insert_bulk(cursor, "legislaciones", data.get("legislaciones", []), ["id", "codigo", "nombre", "descripcion"])
        insert_bulk(cursor, "leyes", data.get("leyes", []), ["id", "legislacion_id", "tipo_norma", "nombre_oficial", "autoridad_emisora"])
        insert_bulk(cursor, "leyes_versiones", data.get("leyes_versiones", []), ["id", "ley_id", "version_label", "es_vigente"])
        
        # ---- 2. Artículos y Relaciones ----
        insert_bulk(cursor, "articulos_ley", data.get("articulos_ley", []), ["id", "ley_version_id", "articulo_numero", "articulo_etiqueta", "texto_oficial", "orden"])
        insert_bulk(cursor, "fragmentos_normativos", data.get("fragmentos_normativos", []), ["id", "ley_id", "ley_version_id", "articulo_id", "tipo_fragmento", "contenido", "orden"])

        conn.commit()
        print("¡Inyección de Leyes exitosa y completa!")
        
    except Exception as e:
        conn.rollback()
        print(f"Error fatal durante inyección, Rollback ejecutado: {e}")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
