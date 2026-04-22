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
    
    # Because Postgres requires dropping the column or casting if changing dimension sizes
    # We can just ALTER TYPE using a cast, but the easiest is simply dropping the vector 
    # columns (since they are empty anyway) and adding them back.
    queries = [
        "ALTER TABLE sentencias DROP COLUMN IF EXISTS embedding;",
        "ALTER TABLE fragmentos_texto DROP COLUMN IF EXISTS embedding_fragmento;",
        "ALTER TABLE sentencias ADD COLUMN embedding VECTOR(1024);",
        "ALTER TABLE fragmentos_texto ADD COLUMN embedding_fragmento VECTOR(1024);"
    ]
    
    for q in queries:
        try:
            cursor.execute(q)
            print(f"Success: {q}")
        except Exception as e:
            print(f"Error on {q}: {e}")
            conn.rollback()
            continue
            
    conn.commit()
    cursor.close()
    conn.close()
    print("Database Vectors resized to 1024 dimensions.")

if __name__ == "__main__":
    main()
