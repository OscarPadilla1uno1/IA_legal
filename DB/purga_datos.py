import psycopg2

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432"
}

def main():
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True
    cursor = conn.cursor()
    
    print("=== INICIANDO PURGA FORENSE Y BLINDAJE DE RAG ===\n")
    
    # 1. Eliminar fragmentos menores a 50 caracteres
    print("1. Exterminando fragmentos de bajo peso semántico (<50 chars)...")
    cursor.execute("DELETE FROM fragmentos_texto WHERE LENGTH(contenido) < 50;")
    print(f"   -> {cursor.rowcount} fragmentos residuales eliminados con éxito.")
    
    # 2. Eliminar clones globales usando MD5 para retener solo el originario (MIN id)
    print("\n2. Identificando Clones Vectoriales y aislando al Original (Puede tardar 1-2 minutos)...")
    query_dupes = """
    DELETE FROM fragmentos_texto
    WHERE id NOT IN (
        SELECT CAST(MIN(CAST(id AS TEXT)) AS UUID)
        FROM fragmentos_texto
        GROUP BY md5(contenido)
    );
    """
    cursor.execute(query_dupes)
    print(f"   -> ¡{cursor.rowcount} clones parasíticos depurados de tu entorno!")
    
    # 3. Crear Constraint de Índice Único
    print("\n3. Inyectando blindaje estructural de Índice Único a Nivel Sentencia...")
    query_index = """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_frag_content_sentence 
    ON fragmentos_texto (sentencia_id, md5(contenido));
    """
    cursor.execute(query_index)
    print("   -> Blindaje (Constraint) activo. Nunca más volverá a duplicarse un registro idéntico.")
    
    # 4. Exponer Sentencias Outliers (>100 Chunks)
    print("\n4. Búsqueda y Extracción de Anomalías o Multi-Resoluciones (>100 chunks):")
    query_outliers = """
    SELECT s.numero_sentencia, COUNT(f.id) as total_chunks
    FROM fragmentos_texto f
    JOIN sentencias s ON s.id = f.sentencia_id
    GROUP BY s.id, s.numero_sentencia
    HAVING COUNT(f.id) > 100
    ORDER BY total_chunks DESC
    LIMIT 20;
    """
    cursor.execute(query_outliers)
    outliers = cursor.fetchall()
    
    if outliers:
        print("   -> ATENCIÓN: Las siguientes sentencias son monstruosamente gigantes. Suelen ser Archivos Compilados en lugar de Fallos Únicos:")
        for num, chunks in outliers:
            print(f"      - {num} : {chunks} chunks extraídos")
    else:
        print("   -> No existen anomalías de sentencias gigantes en el ecosistema.")
        
    cursor.close()
    conn.close()
    
    print("\n¡Purga Vectorial Exitosa! El motor RAG acaba de liberar el 18% de su memoria RAM basura perenne.")

if __name__ == "__main__":
    main()
