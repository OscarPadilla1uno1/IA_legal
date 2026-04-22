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
    
    queries = [
        "ALTER TABLE legislacion ALTER COLUMN nombre_ley DROP NOT NULL;",
        "ALTER TABLE tipos_proceso ALTER COLUMN nombre DROP NOT NULL;",
        "ALTER TABLE magistrados ALTER COLUMN nombre DROP NOT NULL;",
        "ALTER TABLE tribunales ALTER COLUMN nombre DROP NOT NULL;",
        "ALTER TABLE personas_entidades ALTER COLUMN nombre DROP NOT NULL;",
        "ALTER TABLE materias ALTER COLUMN nombre DROP NOT NULL;"
    ]
    
    for q in queries:
        try:
            cursor.execute(q)
        except Exception as e:
            print(f"Error on {q}: {e}")
            conn.rollback()
            continue
            
    conn.commit()
    cursor.close()
    conn.close()
    print("Database limits lifted successfully (VARCHAR -> TEXT).")

if __name__ == "__main__":
    main()
