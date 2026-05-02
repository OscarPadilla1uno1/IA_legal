import psycopg2

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432"
}

def clean_db():
    print("Limpiando tablas relacionales para re-ingesta limpia...")
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    # Lista de tablas a limpiar. El orden importa por las FKs.
    tables = [
        "fragmentos_texto",
        "tesauro",
        "documentos_pdf",
        "sentencias_materias",
        "sentencias_legislacion",
        "sentencias",
        "magistrados",
        "tribunales",
        "tipos_proceso",
        "personas_entidades",
        "materias",
        "legislacion"
    ]
    
    try:
        for table in tables:
            cursor.execute(f"TRUNCATE TABLE {table} CASCADE;")
            print(f" - Tabla {table} truncada.")
        
        conn.commit()
        print("\n¡Base de Datos limpia y lista para recibir datos corregidos!")
    except Exception as e:
        conn.rollback()
        print(f"Error limpiando BD: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    clean_db()
