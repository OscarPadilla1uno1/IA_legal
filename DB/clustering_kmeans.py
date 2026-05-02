import psycopg2
import numpy as np
import json
from sklearn.cluster import KMeans
from collections import defaultdict
import matplotlib.pyplot as plt
import os

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432"
}

def clean_embedding(embedding_str):
    # pgvector devuelve algo como '[0.1, 0.2, ...]'
    return eval(embedding_str)

def main():
    print("Conectando a PostgreSQL para extraer vectores normativos (Códigos de Honduras)...")
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    # Unificación absoluta de todos los vectores de la base de datos (¡Incluyendo Sentencias!)
    sql = """
    SELECT 
        'Artículo de Código: ' || c.nombre_oficial AS fuente,
        a.articulo_etiqueta AS referencia,
        a.embedding::text AS emb
    FROM articulos_codigo a
    JOIN codigos_honduras c ON a.codigo_id = c.id
    WHERE a.embedding IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'Capítulo Macro: ' || c.nombre_oficial AS fuente,
        cap.capitulo_etiqueta AS referencia,
        cap.embedding::text AS emb
    FROM capitulos_codigo cap
    JOIN codigos_honduras c ON cap.codigo_id = c.id
    WHERE cap.embedding IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'La Gaceta Oficial' AS fuente,
        'Edición No. ' || numero_edicion AS referencia,
        embedding::text AS emb
    FROM gacetas_oficiales
    WHERE embedding IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'Ley Suelta: ' || l.nombre_oficial AS fuente,
        al.articulo_etiqueta AS referencia,
        f.embedding_fragmento::text AS emb
    FROM fragmentos_normativos f
    JOIN leyes l ON f.ley_id = l.id
    JOIN articulos_ley al ON f.articulo_id = al.id
    WHERE f.tipo_fragmento = 'articulo' AND f.embedding_fragmento IS NOT NULL

    UNION ALL

    SELECT 
        'Sentencia Corte: ' || COALESCE(s.numero_sentencia, 'S/N') AS fuente,
        'Fragmento ' || f.orden::text AS referencia,
        f.embedding_fragmento::text AS emb
    FROM fragmentos_texto f
    JOIN sentencias s ON f.sentencia_id = s.id
    WHERE f.embedding_fragmento IS NOT NULL;
    """
    
    print("Ejecutando extracción masiva masiva (Puede tomar varios Gigabytes de RAM)...")
    cursor.execute(sql)
    rows = cursor.fetchall()
    
    if not rows:
        print("No se encontraron fragmentos vectorizados.")
        return
        
    print(f"Se extrajeron {len(rows)} artículos masivos para analizar clusters temáticos.")
    
    labels = []
    metadata = []
    matriz_vectores = []
    
    for ley, art, emb_str in rows:
        vec = clean_embedding(emb_str)
        matriz_vectores.append(vec)
        labels.append(f"{ley} - {art}")
        metadata.append({"ley": ley, "art": art})
        
    X = np.array(matriz_vectores)
    
    # K-Means clustering a 8 categorías temáticas sin supervisión
    num_clusters = min(8, len(X))
    print(f"\nIniciando K-Means (k={num_clusters}). El modelo de IA encontrará patrones jurídicos ciegamente...")
    
    kmeans = KMeans(n_clusters=num_clusters, random_state=42, n_init=10)
    kmeans.fit(X)
    
    clusters = defaultdict(list)
    for idx, cluster_label in enumerate(kmeans.labels_):
        clusters[cluster_label].append(metadata[idx])
        
    print("\n=== RESULTADOS DE AGRUPACIÓN (CLUSTERS TEMÁTICOS) ===")
    for cluster_id, items in clusters.items():
        print(f"\n-> Cluster #{cluster_id} ({len(items)} artículos):")
        # Mostrar solo los 3 primeros de cada cluster para no saturar consola
        for item in items[:3]:
            print(f"   - {item['ley']} | {item['art']}")
        if len(items) > 3:
            print(f"   - ... (y {len(items)-3} más)")

    # Gráfica opcional (distancia de los articulos a su centro)
    # Por simplicidad en MVP solo reportaremos éxito.
    print("\n¡Clustering finalizado con éxito! Proceso capaz de encontrar patrones ocultos y proximidad estructural entre distintas leyes automáticamente.")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
