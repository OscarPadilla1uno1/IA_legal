import psycopg2

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432"
}

def check_table(cursor, table_name, column_name):
    try:
        cursor.execute(f"SELECT count(*) FROM {table_name} WHERE {column_name} IS NOT NULL;")
        cnt_notnull = cursor.fetchone()[0]
        cursor.execute(f"SELECT count(*) FROM {table_name} WHERE {column_name} IS NULL;")
        cnt_null = cursor.fetchone()[0]
        print(f"Table: {table_name}")
        print(f"  NOT NULL {column_name}: {cnt_notnull}")
        print(f"  NULL {column_name}: {cnt_null}")
        total = cnt_notnull + cnt_null
        if total > 0:
            print(f"  Progress: {(cnt_notnull/total)*100:.2f}%")
        print("-" * 30)
    except Exception as e:
        print(f"Error checking {table_name}: {e}")

def main():
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    # List of tables and their embedding columns to check
    checks = [
        ("fragmentos_texto", "embedding_fragmento"),
        ("sentencias", "embedding"),
        ("norma_fragmentos", "embedding_fragmento"),
        ("codigos_gacetas_fragmentos", "embedding_fragmento") # Just in case
    ]
    
    for table, col in checks:
        check_table(cursor, table, col)
        
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
