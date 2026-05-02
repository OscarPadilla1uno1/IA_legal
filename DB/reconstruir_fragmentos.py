import os
import re
import uuid
import psycopg2
from psycopg2.extras import execute_batch
import nltk
from nltk.tokenize import sent_tokenize

# Asegurar nltk data
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432"
}

def hard_cut_words(text, max_words=200):
    """
    Toma un string, lo divide por palabras y retorna una lista de subsecuencias 
    cuyo límite estricto es max_words.
    """
    words = text.split()
    chunks = []
    current_chunk = []
    
    for word in words:
        current_chunk.append(word)
        if len(current_chunk) >= max_words:
            chunks.append(" ".join(current_chunk))
            current_chunk = []
            
    if current_chunk:
        chunks.append(" ".join(current_chunk))
        
    return chunks

def extract_safe_chunks(text, max_words=200, overlap_words=40):
    if not text:
        return []
    sentences = sent_tokenize(text, language='spanish')
    safe_sentences = []
    
    # 1. Seguridad: Si nltk extrajo una oración monstruosa, la partimos a la fuerza
    for s in sentences:
        if len(s.split()) > max_words:
            chopped = hard_cut_words(s, max_words=max_words)
            safe_sentences.extend(chopped)
        else:
            safe_sentences.append(s)

    # 2. Reensamblado con solape (Overlap)
    chunks = []
    current_chunk_sents = []
    current_length = 0
    
    for sentence in safe_sentences:
        words_count = len(sentence.split())
        
        # Agregamos esta oración a un chunk
        if current_length > 0 and (current_length + words_count > max_words):
            # Guardar el chunk previo y preparar el solape
            chunks.append(" ".join(current_chunk_sents))
            overlap_sents = []
            overlap_length = 0
            for s in reversed(current_chunk_sents):
                s_len = len(s.split())
                if overlap_length + s_len <= overlap_words or overlap_length == 0:
                    overlap_sents.insert(0, s)
                    overlap_length += s_len
                else:
                    break
            current_chunk_sents = overlap_sents
            current_length = overlap_length
        
        current_chunk_sents.append(sentence)
        current_length += words_count
        
    if current_chunk_sents:
        chunks.append(" ".join(current_chunk_sents))
        
    return chunks

def main():
    print("Conectando a BD para reconstrucción de sentencias...")
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    print("Vaciando la tabla fragmentos_texto de forma segura...")
    cursor.execute("TRUNCATE fragmentos_texto;")
    conn.commit()
    
    print("Cargando textos de sentencias...")
    cursor.execute("SELECT id, texto_integro FROM sentencias WHERE texto_integro IS NOT NULL;")
    rows = cursor.fetchall()
    
    print(f"Comenzando re-fragmentación estricta de {len(rows)} sentencias...")
    
    insert_queries = []
    total_chunks = 0
    
    for row_idx, (sent_id, texto) in enumerate(rows, 1):
        chunks = extract_safe_chunks(texto, max_words=200, overlap_words=40)
        
        for i, chunk_txt in enumerate(chunks, 1):
            frag_id = str(uuid.uuid4())
            insert_queries.append((
                frag_id,
                sent_id,
                i,
                chunk_txt,
                'sentencia'
            ))
            
            if len(insert_queries) >= 3000:
                execute_batch(
                    cursor,
                    "INSERT INTO fragmentos_texto (id, sentencia_id, orden, contenido, tipo_fragmento) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                    insert_queries
                )
                conn.commit()
                total_chunks += len(insert_queries)
                insert_queries.clear()
        
        if row_idx % 1000 == 0:
            print(f" Procesadas {row_idx}/{len(rows)} sentencias... ({total_chunks} fragmentos generados)")
            
    if insert_queries:
        execute_batch(
            cursor,
            "INSERT INTO fragmentos_texto (id, sentencia_id, orden, contenido, tipo_fragmento) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            insert_queries
        )
        conn.commit()
        total_chunks += len(insert_queries)
        
    print(f"Reconstrucción exitosa. Total de fragmentos ultra-seguros creados: {total_chunks}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
