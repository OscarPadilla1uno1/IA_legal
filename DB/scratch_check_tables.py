import psycopg2

DB_PARAMS = {
    "dbname": "legal_ia",
    "user": "root",
    "password": "rootpassword",
    "host": "localhost",
    "port": "5432"
}

try:
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    # Fetch all user tables
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    tables = cursor.fetchall()
    print("Tables in DB:")
    for t in tables:
        print(f" - {t[0]}")
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {t[0]}")
            count = cursor.fetchone()[0]
            print(f"   Count: {count}")
        except Exception as e:
            print(f"   Error count: {e}")
            conn.rollback()

    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error: {e}")
