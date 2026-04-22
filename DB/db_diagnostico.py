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
    cursor = conn.cursor()
    
    print("=== DENSIDAD VECTORIAL Y SALUD DEL RAG ===\n")
    
    # 1. Sentencias Únicas Vectorizadas
    cursor.execute("SELECT COUNT(DISTINCT sentencia_id) FROM fragmentos_texto;")
    unique_sentences = cursor.fetchone()[0]
    print(f"1. Total Sentencias Procesadas: {unique_sentences}")
    
    # 2. Distribución de chunks por sentencia
    cursor.execute("""
    SELECT 
        ROUND(AVG(chunks_por_sentencia), 0)::INT AS promedio,
        MAX(chunks_por_sentencia) AS maximo,
        MIN(chunks_por_sentencia) AS minimo
    FROM (
        SELECT sentencia_id, COUNT(*) AS chunks_por_sentencia
        FROM fragmentos_texto
        GROUP BY sentencia_id
    ) t;
    """)
    avg_chunks, max_chunks, min_chunks = cursor.fetchone()
    print(f"2. Distribución de Chunks (Fragmentos) por Sentencia:")
    print(f"   - Promedio: {avg_chunks} chunks")
    print(f"   - Máximo: {max_chunks} chunks")
    print(f"   - Mínimo: {min_chunks} chunks")
    
    # 3. Chunks sospechosamente cortos (ruido remanente)
    cursor.execute("SELECT COUNT(*) FROM fragmentos_texto WHERE LENGTH(contenido) < 50;")
    short_chunks = cursor.fetchone()[0]
    print(f"3. Peligro Semántico (Menos de 50 caracteres): {short_chunks} fragmentos")
    
    # 4. Posibles duplicados exactos
    cursor.execute("SELECT COUNT(*) - COUNT(DISTINCT contenido) FROM fragmentos_texto;")
    exact_duplicates = cursor.fetchone()[0]
    print(f"4. Chunks con duplicidad exacta: {exact_duplicates} ocurrencias")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
