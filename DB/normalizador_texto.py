import re
import uuid
import psycopg2
from psycopg2.extras import execute_values

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432"
}

MAX_WORDS = 300
OVERLAP = 50

def sanitize_text(text):
    # 1. Eliminar salto de línea con guión (ej: "liber-\ntad" -> "libertad")
    text = re.sub(r'-\s*\n\s*', '', text)
    # 2. Las palabras que saltan de línea sin guión, simplemente ponemos espacio
    text = re.sub(r'\s*\n\s*', ' ', text)
    # 3. Eliminar intros jurídicas puramente burocráticas como "CONSIDERANDO OCHO (8): Que por "
    # Cubre "CONSIDERANDO ... : Que "
    text = re.sub(r'^\s*CONSIDERANDO(?:\s+[A-Z0-9]+)?(?:\s*\(\d+\))?\s*[:\.]\s*(?:Que\s)?', '', text, flags=re.IGNORECASE)
    # 4. Remover espacios extra
    text = re.sub(r'\s{2,}', ' ', text)
    return text.strip()

def chunk_text(text, max_words=MAX_WORDS, overlap=OVERLAP):
    if len(text.strip()) < 50:
        return []
    words = text.split()
    if len(words) <= max_words:
        return [text]
    
    chunks = []
    i = 0
    while i < len(words):
        chunk_words = words[i : i + max_words]
        chunks.append(" ".join(chunk_words))
        i += max_words - overlap
    return chunks

def main():
    print("Iniciando Motor de Sanitización y Re-Chunking Lógico...")
    conn = psycopg2.connect(**DB_PARAMS)
    
    # Cursor en el servidor para evitar traer 340k rows a la RAM local
    fetch_cursor = conn.cursor(name='fetch_cursor', withhold=True)
    fetch_cursor.itersize = 20000
    fetch_cursor.execute("SELECT id, sentencia_id, tipo_fragmento, contenido FROM fragmentos_texto;")
    
    write_cursor = conn.cursor()
    
    inserts = []
    deletes = []
    updates = []
    
    processed = 0
    print("Escaneando base de datos y recomponiendo textos...")
    
    for row in fetch_cursor:
        fid, sid, tipo, raw_text = row
        clean = sanitize_text(raw_text)
        
        # Calcular los bloques matemáticos puros para el modelo
        chunks = chunk_text(clean)
        
        if len(chunks) == 1:
            # Si cabía perfecto, solo actualizamos el texto y borramos su viejo Vector corrupto
            updates.append((clean, str(fid)))
        else:
            # Si era gigante o tóxico, lo borramos y nacen hijos nuevos
            deletes.append((str(fid),))
            for chunk in chunks:
                inserts.append((str(uuid.uuid4()), sid, tipo, chunk))
                
        processed += 1
        if processed % 10000 == 0:
            print(f"[{processed}] Analizados...")
            # Commit batches
            if updates:
                execute_values(write_cursor, 
                    "UPDATE fragmentos_texto AS f SET contenido = v.c, embedding_fragmento = NULL FROM (VALUES %s) AS v(c, id) WHERE f.id = CAST(v.id AS UUID)",
                    updates)
                updates.clear()
            if deletes:
                write_cursor.executemany("DELETE FROM fragmentos_texto WHERE id = CAST(%s AS UUID)", deletes)
                deletes.clear()
            if inserts:
                execute_values(write_cursor, 
                    "INSERT INTO fragmentos_texto (id, sentencia_id, tipo_fragmento, contenido) VALUES %s", 
                    inserts)
                inserts.clear()
            conn.commit()

    # Final flush
    if updates:
        execute_values(write_cursor, "UPDATE fragmentos_texto AS f SET contenido = v.c, embedding_fragmento = NULL FROM (VALUES %s) AS v(c, id) WHERE f.id = CAST(v.id AS UUID)", updates)
    if deletes:
        write_cursor.executemany("DELETE FROM fragmentos_texto WHERE id = CAST(%s AS UUID)", deletes)
    if inserts:
        execute_values(write_cursor, "INSERT INTO fragmentos_texto (id, sentencia_id, tipo_fragmento, contenido) VALUES %s", inserts)
    conn.commit()

    write_cursor.close()
    fetch_cursor.close()
    conn.close()
    
    print("\n¡Sanitización y Smart-Chunking exitoso! Todos los fragmentos fueron acoplados a la máxima expresión de Ollama BGE-M3.")
    print("Nótonse que los Vectores en la DB han sido borrados temporalmente para que vuelvas a lanzar el Inyector Ollama.")

if __name__ == "__main__":
    main()
